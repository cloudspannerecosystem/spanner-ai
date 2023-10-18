from google.cloud import spanner
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.workflows import executions_v1
from google.cloud.workflows.executions_v1 import Execution
from google.cloud import workflows_v1
import random, json, string, time, pytest


PROJECT_ID = "span-cloud-testing"
SPANNER_INSTANCE_ID = "mlops-testing"
SPANNER_DATABASE_ID = "vector-db-load-test"
SPANNER_TABLE_NAME = "test_spanner_vertex_vector_integration_" + str(random.randint(10000, 99999))
WORKFLOW_NAME = "test-spanner-vvi-" + str(random.randint(10000, 99999))
WORKFLOW_LOCATION = "us-central1"
WORKFLOW_ARGUMENTS_FILE_PATH = "workflow-arguments.json"
INDEX_ENDPOINT = ""
VERTEX_VECTOR_SEARCH_INDEX = "8496735588383195136"


def generate_vector_data(number_of_rows, vector_dimension):
    rows = []

    for i in range(number_of_rows):
        row = ()
        row += (i,)

        # Generating random vector embeddings
        row += ([random.uniform(0, 1) for _ in range(vector_dimension)],)

        # Generate a random sentence with up to 200 words
        max_words = 200
        random_sentence = ' '.join(''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(1, 10))) for _ in range(random.randint(1, max_words)))
        row += (random_sentence,);

        #Restricts
        restricts = JsonObject(
                        [
                            JsonObject({"allow_list": ["even"], "namespace": "class"}),
                        ]
                    )
        row += (restricts,);

        rows.append(row)

    return rows

def setup_spanner(project_id, instance_id, database_id, table_name):
    NUMBER_OF_ROWS_IN_SPANNER  = 1000
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
    ) PRIMARY KEY (id)""".format(tableName=table_name)

    print (ddl)

    databaseoperation = database.update_ddl(
        [
           ddl
        ]
    )

    print("Waiting for creation of Spanner Table...")
    databaseoperation.result(100000)

    print("Created {} table on database {}".format(table_name,database.name))


    rows = generate_vector_data(NUMBER_OF_ROWS_IN_SPANNER, VECTOR_DIMENSION)

    with database.batch() as batch:
        batch.insert(
            table=table_name,
            columns=("id", "embeddings", "text", "restricts"),
            values=rows,
        )

    print("Inserted {} records in table {}".format(NUMBER_OF_ROWS_IN_SPANNER, table_name))

    return rows


def deploy_workflow(project, location, workflow_name):
    file_content = "";

    with open("../workflows/batch-export.yaml", 'r') as file:
        # Read the entire file content
        file_content = file.read()

   # Create a client
    client = workflows_v1.WorkflowsClient()

    # Initialize request argument(s)
    workflow = workflows_v1.Workflow()
    workflow.source_contents = file_content

    request = workflows_v1.CreateWorkflowRequest(
        parent="projects/{project}/locations/{location}".format(project=project, location=location),
        workflow=workflow,
        workflow_id=workflow_name
    )

    # Make the request
    operation = client.create_workflow(request=request)

    print("Waiting for deployment of workflow to complete...")

    response = operation.result()

    print ("Workflow Deployed")


def execute_workflow(project, location, workflow_name):
    client = executions_v1.ExecutionsClient()

    json_arguments = "";

    with open(WORKFLOW_ARGUMENTS_FILE_PATH, 'r') as file:
        # Read the entire file content
       json_arguments = json.load(file)

    json_arguments['project_id'] = PROJECT_ID
    json_arguments['location'] = WORKFLOW_LOCATION
    json_arguments['spanner']['instance_id'] = SPANNER_INSTANCE_ID
    json_arguments['spanner']['database_id'] = SPANNER_DATABASE_ID
    json_arguments['spanner']['table_name'] = SPANNER_TABLE_NAME
    json_arguments['vertex']['vector_search_index_id'] = VERTEX_VECTOR_SEARCH_INDEX

    workflow_execution_request  = Execution();
    workflow_execution_request.argument = json.dumps(json_arguments, indent=4)


    # Initialize request argument(s)
    request = executions_v1.CreateExecutionRequest(
        parent="projects/{project}/locations/{location}/workflows/{workflow_name}".format(project=project, location=location, workflow_name=workflow_name),
        execution=workflow_execution_request
    )

    response =  client.create_execution(request=request)

    print("Execution of workflow triggered with following arguments: {}".format(json_arguments))

    return response


def get_worfklow_execution(arguments):
    client = executions_v1.ExecutionsClient()

    # Initialize request argument(s)
    request = executions_v1.GetExecutionRequest(
        name=arguments['execution_id'],
    )

    # Make the request
    response = client.get_execution(request=request)

    # Handle the response
    return response

def workflow_execution_polling_predicate(workflow_execution_response):
    if workflow_execution_response.state != Execution.State.ACTIVE:
        return True

    return False


def polling(function_to_poll, arguments, function_poll_predicate, max_attempts=100, polling_interval=120):
    for attempt in range(max_attempts):
        response = function_to_poll(arguments)

        if function_poll_predicate(response):
            return response  # Desired condition met

        print(f"Attempt {attempt + 1}: Workflow execution in progress, waiting for workflow to finish..")
        time.sleep(polling_interval)

    raise TimeoutError("Polling timed out")


def sync_execute_workflow(project, location, workflow_name):

    execute_workflow_response = execute_workflow(project, location, workflow_name)

    try:
        result = polling(get_worfklow_execution, {'execution_id': execute_workflow_response.name}, workflow_execution_polling_predicate)
        print("Desired condition met:", result)
    except TimeoutError:
        print("Polling timed out. Desired condition not met.")


def cleanup(project_id, instance_id, database_id, table_name):
    spanner_client = spanner.Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    database.update_ddl(["DROP TABLE " + table_name])

@pytest.fixture
def rows():
    # Setup code, e.g., initialize resources
    print("\nSetup for integration test")
    #1 Setting up Spanner
    rows = setup_spanner(PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID, SPANNER_TABLE_NAME)

    try:
        # Deploy Workflow
        deploy_workflow(PROJECT_ID, WORKFLOW_LOCATION, WORKFLOW_NAME)

        #Execute Workflow
        sync_execute_workflow(PROJECT_ID, WORKFLOW_LOCATION, WORKFLOW_NAME)

    except BaseException:
        cleanup(PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID, SPANNER_TABLE_NAME)

    yield rows # This is where the test runs

    cleanup(PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID, SPANNER_TABLE_NAME)

    # Teardown code, e.g., clean up resources
    print("Teardown after integration test")


def testJob(rows):
    print (rows)




