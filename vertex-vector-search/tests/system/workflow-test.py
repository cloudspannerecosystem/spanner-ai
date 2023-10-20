# Copyright 2023 Google Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import spanner
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.workflows import executions_v1
from google.cloud.workflows.executions_v1 import Execution
from google.cloud import workflows_v1
import random, json, string, time, pytest, os
import logging
from google.cloud import aiplatform_v1beta1


# Configure Variables
PROJECT_ID = "span-cloud-testing"
SPANNER_INSTANCE_ID = "vertex-vector-search-tests"
SPANNER_DATABASE_ID = "batch-vector-export"
SPANNER_TABLE_NAME = "test_spanner_vertex_vector_integration_" + str(
    random.randint(10000, 99999)
)
WORKFLOW_NAME = "test-spanner-vvi-" + str(random.randint(10000, 99999))
WORKFLOW_LOCATION = "us-central1"
VERTEX_VECTOR_SEARCH_INDEX_ENDPOINT = "1973409086.us-central1-545418958905.vdb.vertexai.goog"
VERTEX_VECTOR_SEARCH_INDEX = "8496735588383195136"

# Get the directory where this test file is located
THIS_FILE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_INPUT_FILE_PATH = THIS_FILE_DIRECTORY + "/workflow-input.json"
WORKFLOW_YAML_FILE_PATH = (
    os.path.dirname(os.path.dirname(THIS_FILE_DIRECTORY))
    + "/workflows/batch-export.yaml"
)


# Define a custom log format
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"

# Create a logger instance
logger = logging.getLogger(__name__)

# Set the log level to capture all log messages
logger.setLevel(level=logging.DEBUG)

# Create a handler for console output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format, log_datefmt))

# Add the console handler and file handler to the logger
logger.addHandler(console_handler)


def generate_vector_data(number_of_rows, vector_dimension):
    """Generates vector data for Spanner table.

    Args:
        number_of_rows: The number of rows to generate.
        vector_dimension: The dimension of the vectors.

    Returns:
        A list of rows, each of which is a tuple of (id, text, embeddings, restricts).
    """

    logger.info(
        "Generating {} vector embeddings each of dimension: {}.".format(
            number_of_rows, vector_dimension
        )
    )

    rows = []

    for i in range(number_of_rows):
        row = ()
        row += (i,)

        # Generating random vector embeddings
        row += ([random.uniform(0, 1) for _ in range(vector_dimension)],)

        # Generate a random sentence with up to 200 words
        max_words = 200
        random_sentence = " ".join(
            "".join(
                random.choice(string.ascii_lowercase)
                for _ in range(random.randint(1, 10))
            )
            for _ in range(random.randint(1, max_words))
        )
        row += (random_sentence,)

        # Restricts
        restricts = JsonObject(
            [
                JsonObject({"allow_list": ["even"], "namespace": "class"}),
            ]
        )
        row += (restricts,)

        rows.append(row)

    logger.info("Vector Embeddings generated.")

    return rows


def setup_spanner(project_id, instance_id, database_id, table_name):
    """Sets up a Spanner table with vector embeddings.

    Args:
        project_id: The project ID.
        instance_id: The instance ID.
        database_id: The database ID.
        table_name: The table name.
    """

    logger.info("Setting up Spanner Table...")

    NUMBER_OF_ROWS_IN_SPANNER = 1000
    VECTOR_DIMENSION = 128

    spanner_client = spanner.Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    ddl = """CREATE TABLE {tableName} (
        id INT64 NOT NULL,
        text STRING(MAX),
        embeddings ARRAY<FLOAT64>,
        restricts JSON,
        crowding_tag STRING(MAX),
    ) PRIMARY KEY (id)""".format(
        tableName=table_name
    )

    logger.info("Spanner DDL to create new table: {}.".format(ddl))

    databaseoperation = database.update_ddl([ddl])

    logger.info("Waiting for creation of Spanner Table...")
    databaseoperation.result(100000)

    logger.info("Created {} table on database {}.".format(table_name, database.name))

    rows = generate_vector_data(NUMBER_OF_ROWS_IN_SPANNER, VECTOR_DIMENSION)

    logger.info(
        "Inserting generated vector embeddings in Spanner Table: {}.".format(table_name)
    )

    with database.batch() as batch:
        batch.insert(
            table=table_name,
            columns=("id", "embeddings", "text", "restricts"),
            values=rows,
        )

    logger.info(
        "Inserted {} records in table {}.".format(NUMBER_OF_ROWS_IN_SPANNER, table_name)
    )

    return rows


def deploy_workflow(project, location, workflow_name):
    """Deploys a workflow defined in file "https://github.com/cloudspannerecosystem/spanner-ai/vertex-vector-search/workflows/batch-export.yaml" to Cloud Workflow.

    Args:
        project: The project ID.
        location: The location of the workflow.
        workflow_name: The name of the workflow.
    """
    logger.info(
        "Deploying workflow with name: {} on project: {} and location: {}.".format(
            workflow_name, project, location
        )
    )
    logger.info(
        "Picking workflow configuration from following path: {}.".format(
            WORKFLOW_YAML_FILE_PATH
        )
    )
    file_content = ""

    with open(WORKFLOW_YAML_FILE_PATH, "r") as file:
        # Read the entire file content
        file_content = file.read()

    # Create a client
    client = workflows_v1.WorkflowsClient()

    # Initialize request argument(s)
    workflow = workflows_v1.Workflow()
    workflow.source_contents = file_content

    request = workflows_v1.CreateWorkflowRequest(
        parent="projects/{project}/locations/{location}".format(
            project=project, location=location
        ),
        workflow=workflow,
        workflow_id=workflow_name,
    )

    # Make the request
    operation = client.create_workflow(request=request)

    logger.info("Waiting for deployment of workflow to complete...")

    response = operation.result()

    logger.info(
        "Workflow with name: {} deployed successfully on project: {}.".format(
            workflow_name, project
        )
    )


def execute_workflow(project, location, workflow_name):
    """Executes a workflow.

    Args:
        project: The project ID.
        location: The location of the workflow.
        workflow_name: The name of the workflow.
    """
    logger.info("Starting execution of workflow with name: {}.".format(workflow_name))

    client = executions_v1.ExecutionsClient()

    json_arguments = ""

    logger.info(
        "Reading workflow input template json from: {}.".format(
            WORKFLOW_INPUT_FILE_PATH
        )
    )

    with open(WORKFLOW_INPUT_FILE_PATH, "r") as file:
        # Read the entire file content
        json_arguments = json.load(file)

    json_arguments["project_id"] = PROJECT_ID
    json_arguments["location"] = WORKFLOW_LOCATION
    json_arguments["spanner"]["instance_id"] = SPANNER_INSTANCE_ID
    json_arguments["spanner"]["database_id"] = SPANNER_DATABASE_ID
    json_arguments["spanner"]["table_name"] = SPANNER_TABLE_NAME
    json_arguments["vertex"]["vector_search_index_id"] = VERTEX_VECTOR_SEARCH_INDEX

    workflow_execution_request = Execution()
    workflow_execution_request.argument = json.dumps(json_arguments, indent=4)

    # Initialize request argument(s)
    request = executions_v1.CreateExecutionRequest(
        parent="projects/{project}/locations/{location}/workflows/{workflow_name}".format(
            project=project, location=location, workflow_name=workflow_name
        ),
        execution=workflow_execution_request,
    )

    response = client.create_execution(request=request)

    logger.info(
        "Execution of workflow with name: {} triggered with following arguments: {}.".format(
            workflow_name, json_arguments
        )
    )

    return response


def get_worfklow_execution(arguments):
    """Gets a workflow execution.

    Args:
        arguments: A dictionary of arguments containg the `execution_id`.

    Returns:
        A workflow execution.
    """

    logger.info(
        "Fetching execution status of workflow with id: {}.".format(
            arguments["execution_id"]
        )
    )
    client = executions_v1.ExecutionsClient()

    # Initialize request argument(s)
    request = executions_v1.GetExecutionRequest(
        name=arguments["execution_id"],
    )

    # Make the request
    response = client.get_execution(request=request)

    # Handle the response
    return response


def workflow_execution_polling_predicate(workflow_execution_response):
    """A predicate that determines whether a workflow execution has finished.
    Checks whether the workflow state is `Active` or not.

    Args:
        workflow_execution_response: A workflow execution.

    Returns:
        True if the workflow execution has finished, False otherwise.
    """
    if workflow_execution_response.state != Execution.State.ACTIVE:
        return True

    return False


def polling(
    function_to_poll,
    arguments,
    function_poll_predicate,
    max_attempts=100,
    polling_interval=120,
):
    """A polling function that polls a function until a predicate is met.

    Args:
        function_to_poll: A function to poll.
        arguments: A dictionary of arguments to pass to the function to poll.
        function_poll_predicate: A predicate that determines whether the polling should stop.
        max_attempts: The maximum number of attempts to poll.
        polling_interval: The interval between polls.

    Returns:
        The result of the function to poll.
    """
    for attempt in range(max_attempts):
        response = function_to_poll(arguments)

        if function_poll_predicate(response):
            return response  # Desired condition met

        logger.info(
            "Attempt {}: Workflow execution in progress, waiting for workflow to finish...".format(
                attempt + 1
            )
        )
        time.sleep(polling_interval)

    raise TimeoutError("Polling timed out")


def sync_execute_workflow(project, location, workflow_name):
    """Synchronously executes a workflow.

    Args:
        project: The project ID.
        location: The location of the workflow.
        workflow_name: The name of the workflow.
    """
    execute_workflow_response = execute_workflow(project, location, workflow_name)

    try:
        result = polling(
            get_worfklow_execution,
            {"execution_id": execute_workflow_response.name},
            workflow_execution_polling_predicate,
        )
        logger.info("Workflow exeuction finished with result: {}.".format(result))
    except TimeoutError:
        logger.error("Workflow exeuction polling timed out.")


def cleanup(project_id, instance_id, database_id, table_name, workflow_name, location):
    """Cleans up the resources which includes Spanner Table & Cloud Workflows .

    Args:
        project_id: The project ID.
        instance_id: The instance ID.
        database_id: The database ID.
        table_name: The table name.
        workflow_name: The workflow name
    """

    logger.info("Cleaning up Spanner & Workflow resources")
    spanner_client = spanner.Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    database.update_ddl(["DROP TABLE " + table_name])

    logger.info("Dropped Spanner table with name: {}.".format(table_name))

    workflow_client = workflows_v1.WorkflowsClient()

    workflow_full_path = (
        "projects/{project}/locations/{location}/workflows/{workflow_name}".format(
            project=project_id, location=location, workflow_name=workflow_name
        )
    )

    # Initialize request argument(s)
    request = workflows_v1.DeleteWorkflowRequest(
        name=workflow_full_path,
    )

    # Make the request
    operation = workflow_client.delete_workflow(request=request)

    logger.info("Delete Cloud Workflow with name: {}.".format(workflow_full_path))

    response = operation.result()


def read_index_datapoints(api_endpoint, keys):
    """Reads datapoints from a deployed Vertex Index.

    Args:
      api_endpoint: The AI Platform Index API endpoint.
      keys: A list of datapoint IDs to fetch.

    Returns:
      A ReadIndexDatapointsResponse.
    """
    # Create a client
    client_options = {"api_endpoint": api_endpoint}

    client = aiplatform_v1beta1.MatchServiceClient(client_options=client_options)

    # Initialize request argument(s)
    request = aiplatform_v1beta1.ReadIndexDatapointsRequest(
        deployed_index_id="spanner_vvs_batch_integration_test_suite", ids=keys
    )

    # Make the request
    response = client.read_index_datapoints(request=request)

    # Handle the response
    return response


@pytest.fixture
def spanner_vertex_vector_search_data():
    """
    Setting up Spanner Table with vector embeddings to test the workflow.
    The function does following operations:
    1. Creation of Spanner table.
    2. Inserting randomly generated vector embeddings data into spanner table.
    3. Invoke the test to execute workflow and comapre vector embeddings.
    4. Tear down Resources:
        a. Dropping Spanner Table
        b. Delete Cloud Workflow
    """
    # Setup code, e.g., initialize resources
    logger.info("Setting up resources for Integration Tests")

    # 1 Setting up Spanner
    try:
        rows = setup_spanner(
            PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID, SPANNER_TABLE_NAME
        )
    except Exception as e:
        logger.error(
            "An exception occurred while setting up Spanner table: %s",
            str(e),
            exc_info=True,
        )
        pytest.fail(
            "Test failed due to unhandled exception while setting up spanner table."
        )

    try:
        yield rows  # This is where the test runs
    except Exception as e:
        logger.error(
            "An exception occurred while deploying/executing workflow: %s",
            str(e),
            exc_info=True,
        )

    # Teardown code, e.g., clean up resources
    logger.info("Teardown resources.")
    cleanup(
        PROJECT_ID,
        SPANNER_INSTANCE_ID,
        SPANNER_DATABASE_ID,
        SPANNER_TABLE_NAME,
        WORKFLOW_NAME,
        WORKFLOW_LOCATION,
    )


def testSpannerVertexVectorSearchIntegration(spanner_vertex_vector_search_data):
    """
    Tests integration between Spanner and Vertex Vector Search.
    1. Deploy workflow to Console.
    2. Execute the workflow synchronously.
    3. Fetch Vector Embeddings from Vertex Index.
    4. Compare generated embeddings from the embeddings in Vertex Index.
    """

    # Deploy Workflow
    deploy_workflow(PROJECT_ID, WORKFLOW_LOCATION, WORKFLOW_NAME)

    # Execute Workflow
    sync_execute_workflow(PROJECT_ID, WORKFLOW_LOCATION, WORKFLOW_NAME)

    # Dictionary from id -> row
    spanner_vertex_vector_search_data_dict = {
        item[0]: item for item in spanner_vertex_vector_search_data
    }

    data_point_id_list = list(spanner_vertex_vector_search_data_dict.keys())
    data_point_id_list = [
        str(key) for key in data_point_id_list
    ]  # Convert keys to strings

    # Fetching data from Vertex Index
    vertex_vector_search_data = read_index_datapoints(
        VERTEX_VECTOR_SEARCH_INDEX_ENDPOINT, data_point_id_list
    )

    for data_point in vertex_vector_search_data.datapoints:
        actual_data = spanner_vertex_vector_search_data_dict.get(
            int(data_point.datapoint_id), None
        )

        assert actual_data is not None

        actual_vector_embeddings = actual_data[1]
        vertex_index_vector_embeddings = list(data_point.feature_vector)

        assert actual_vector_embeddings == vertex_index_vector_embeddings
