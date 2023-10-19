import nox

@nox.session(python=["3.11"])
def tests(session):
  # Install dependencies
  session.install("-r", "requirements.txt")

  # Run your integration tests
  session.run("pytest", "-s", "system/workflow-test.py")
