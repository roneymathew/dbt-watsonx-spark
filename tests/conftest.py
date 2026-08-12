import pytest
import os
from datetime import datetime

pytest_plugins = ["dbt.tests.fixtures.project"]

# Module-level variables for log directory management
_test_run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
_test_class_counter = {}


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="apache_spark", type=str)


@pytest.fixture(scope="class")
def logs_dir(request):
    """
    Override dbt's logs_dir fixture to create organized log directories.
    
    Creates a unique directory for each test class in format:
    logs/<timestamp>/<TestClassName>_<counter>/
    
    This ensures:
    1. Each test run gets a timestamped parent directory
    2. Each test class gets its own subdirectory
    3. Multiple runs of the same test class get unique directories via counter
    
    IMPORTANT: Sets DBT_LOG_PATH environment variable so dbt writes logs to our custom directory.
    """
    # Get the test class name
    test_class_name = request.cls.__name__ if request.cls else "unknown"
    
    # Increment counter for this test class
    if test_class_name not in _test_class_counter:
        _test_class_counter[test_class_name] = 0
    _test_class_counter[test_class_name] += 1
    
    # Create directory name with counter
    dir_name = f"{test_class_name}_{_test_class_counter[test_class_name]}"
    
    # Get absolute path to workspace root
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Create full path: workspace/logs/<timestamp>/<TestClass>_<counter>
    log_dir = os.path.join(workspace_root, "logs", _test_run_timestamp, dir_name)
    
    # Create the directory
    os.makedirs(log_dir, exist_ok=True)
    
    # Set environment variable so dbt uses our custom log directory
    os.environ["DBT_LOG_PATH"] = str(log_dir)
    
    # Yield the path (required by dbt's test framework)
    yield str(log_dir)
    
    # Cleanup: remove environment variable after test
    if "DBT_LOG_PATH" in os.environ:
        del os.environ["DBT_LOG_PATH"]


# Using @pytest.mark.skip_profile('apache_spark') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "databricks_cluster":
        target = databricks_cluster_target()
    elif profile_type == "databricks_sql_endpoint":
        target = databricks_sql_endpoint_target()
    elif profile_type == "apache_spark":
        target = apache_spark_target()
    elif profile_type == "databricks_http_cluster":
        target = databricks_http_cluster_target()
    elif profile_type == "spark_session":
        target = spark_session_target()
    elif profile_type == "watsonx_test":
        target = watsonx_target()  # Use same config as watsonx
    elif profile_type == "watsonx_authz_test":
        target = watsonx_target()  # Use same config as watsonx
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def apache_spark_target():
    return {
        "type": "spark",
        "host": "spark_db",
        "user": "dbt",
        "method": "thrift",
        "port": 10000,
        "connect_retries": 2,
        "connect_timeout": 3,
        "retry_all": False,
    }


def databricks_cluster_target():
    return {
        "type": "spark",
        "method": "odbc",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "cluster": os.getenv("DBT_DATABRICKS_CLUSTER_NAME"),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "driver": os.getenv("ODBC_DRIVER"),
        "port": 443,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": False,
        "user": os.getenv("DBT_DATABRICKS_USER"),
    }


def databricks_sql_endpoint_target():
    return {
        "type": "spark",
        "method": "odbc",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "endpoint": os.getenv("DBT_DATABRICKS_ENDPOINT"),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "driver": os.getenv("ODBC_DRIVER"),
        "port": 443,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": True,
    }


def databricks_http_cluster_target():
    return {
        "type": "spark",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "cluster": os.getenv("DBT_DATABRICKS_CLUSTER_NAME"),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "method": "http",
        "port": 443,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": False,
        "user": os.getenv("DBT_DATABRICKS_USER"),
    }


def spark_session_target():
    return {
        "type": "spark",
        "host": "localhost",
        "method": "session",
    }


def watsonx_target():
    """Target configuration for watsonx.data
    
    Reads from environment variables set by the test automation script.
    Falls back to defaults only if env vars are not set.
    """
    return {
        "type": "watsonx_spark",
        "method": "http",
        "host": os.getenv("WATSONX_HOST", "https://cpd-cpd-instance.apps.ingestion-big04.cp.fyre.ibm.com"),
        # WATSONX_URI should be set by the automation script to point to the correct query server
        "uri": os.getenv("WATSONX_URI", "/lakehouse/api/v3/spark_engines/spark613/query_servers/329c8056-8582-4958-bd2b-50f76cbdfee0/connect/cliservice"),
        "catalog": os.getenv("WATSONX_CATALOG", "iceberg_data"),
        "schema": os.getenv("WATSONX_SCHEMA", "default"),
        "use_ssl": False,
        "suppress_ssl_warnings": True,
        "auth": {
            "apikey": os.getenv("WATSONX_APIKEY", ""),
            "instance": os.getenv("WATSONX_INSTANCE", "1777813222451474"),
            "user": os.getenv("WATSONX_USER", "admin"),
        },
        "connect_retries": 3,
        "connect_timeout": 10,
        "retry_all": False,
        # Enable aggressive schema drop for tests (required for Iceberg cleanup)
        "enable_aggressive_schema_drop": True,
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")


@pytest.fixture(scope="class")
def cleanup_watsonx_schemas(request):
    """
    Custom cleanup for watsonx/Iceberg schemas.
    Iceberg requires dropping all tables before dropping the schema.
    """
    yield
    
    profile_type = request.config.getoption("--profile")
    if profile_type == "watsonx":
        # Get the project fixture
        project = request.getfixturevalue("project")
        
        # Get all schemas used in the test
        unique_schemas = request.getfixturevalue("unique_schema")
        test_schema = f"{project.test_schema}_{unique_schemas}"
        
        try:
            # First, drop all relations in the schema
            from dbt.adapters.contracts.relation import RelationType
            
            # Create a relation for the schema
            schema_relation = project.adapter.Relation.create(
                database=None,
                schema=test_schema
            )
            
            # List all relations in the schema
            relations = project.adapter.list_relations_without_caching(schema_relation)
            
            # Drop each relation
            for relation in relations:
                try:
                    if relation.type == RelationType.View:
                        project.adapter.drop_relation(relation)
                    else:
                        project.adapter.drop_relation(relation)
                except Exception as e:
                    # Log but don't fail on individual relation drops
                    print(f"Warning: Could not drop {relation}: {e}")
            
            # Now drop the schema
            project.adapter.drop_schema(schema_relation)
            
        except Exception as e:
            # Log but don't fail the test on cleanup errors
            print(f"Warning: Schema cleanup failed: {e}")
