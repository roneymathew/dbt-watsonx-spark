import unittest
from unittest.mock import Mock, patch, MagicMock
import time
import base64
import uuid

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.watsonx_spark.python_submissions import (
    BaseDatabricksHelper,
    JobClusterPythonJobHelper,
    AllPurposeClusterPythonJobHelper,
    DBContext,
    DBCommand,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_TIMEOUT,
)


class MockCredentials:
    """Mock SparkCredentials for testing"""
    def __init__(self, host="test.databricks.com", token="test_token", cluster_id=None):
        self.host = host
        self.token = token
        self.cluster_id = cluster_id


class TestBaseDatabricksHelper(unittest.TestCase):
    """Test the BaseDatabricksHelper class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.credentials = MockCredentials()
        self.parsed_model = {
            "alias": "test_model",
            "schema": "test_schema",
            "config": {}
        }
    
    def test_init(self):
        """Test BaseDatabricksHelper initialization"""
        # Need to create a concrete subclass since BaseDatabricksHelper is abstract
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        self.assertEqual(helper.credentials, self.credentials)
        self.assertEqual(helper.identifier, "test_model")
        self.assertEqual(helper.schema, "test_schema")
        self.assertEqual(helper.timeout, DEFAULT_TIMEOUT)
        self.assertEqual(helper.polling_interval, DEFAULT_POLLING_INTERVAL)
        self.assertIn("Authorization", helper.auth_header)
        self.assertIn("Bearer test_token", helper.auth_header["Authorization"])
    
    def test_cluster_id_from_model_config(self):
        """Test cluster_id property from model config"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        self.parsed_model["config"]["cluster_id"] = "model_cluster_123"
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        self.assertEqual(helper.cluster_id, "model_cluster_123")
    
    def test_cluster_id_from_credentials(self):
        """Test cluster_id property from credentials"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        self.credentials.cluster_id = "cred_cluster_456"
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        self.assertEqual(helper.cluster_id, "cred_cluster_456")
    
    def test_get_timeout_default(self):
        """Test get_timeout with default value"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        self.assertEqual(helper.timeout, DEFAULT_TIMEOUT)
    
    def test_get_timeout_custom(self):
        """Test get_timeout with custom value"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        self.parsed_model["config"]["timeout"] = 3600
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        self.assertEqual(helper.timeout, 3600)
    
    def test_get_timeout_invalid(self):
        """Test get_timeout with invalid value raises error"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        self.parsed_model["config"]["timeout"] = -1
        
        with self.assertRaises(ValueError) as cm:
            ConcreteHelper(self.parsed_model, self.credentials)
        self.assertIn("positive integer", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_create_work_dir_success(self, mock_post):
        """Test _create_work_dir with successful response"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        helper._create_work_dir("/test/path")
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("/workspace/mkdirs", call_args[0][0])
        self.assertEqual(call_args[1]["json"]["path"], "/test/path")
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_create_work_dir_failure(self, mock_post):
        """Test _create_work_dir with failed response"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error message"
        mock_post.return_value = mock_response
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper._create_work_dir("/test/path")
        self.assertIn("Error creating work_dir", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_upload_notebook_success(self, mock_post):
        """Test _upload_notebook with successful response"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        code = "print('hello')"
        helper._upload_notebook("/test/notebook", code)
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("/workspace/import", call_args[0][0])
        
        # Verify base64 encoding
        expected_content = base64.b64encode(code.encode()).decode()
        self.assertEqual(call_args[1]["json"]["content"], expected_content)
        self.assertEqual(call_args[1]["json"]["language"], "PYTHON")
        self.assertTrue(call_args[1]["json"]["overwrite"])
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_upload_notebook_failure(self, mock_post):
        """Test _upload_notebook with failed response"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error"
        mock_post.return_value = mock_response
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper._upload_notebook("/test/notebook", "code")
        self.assertIn("Error creating python notebook", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.uuid.uuid4')
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_submit_job_success(self, mock_post, mock_uuid):
        """Test _submit_job with successful response"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_uuid.return_value = "test-uuid"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"run_id": "12345"}
        mock_post.return_value = mock_response
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        run_id = helper._submit_job("/test/path", {"new_cluster": {}})
        
        self.assertEqual(run_id, "12345")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("/jobs/runs/submit", call_args[0][0])
        self.assertIn("test_schema-test_model", call_args[1]["json"]["run_name"])
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_submit_job_with_packages(self, mock_post):
        """Test _submit_job with packages"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"run_id": "12345"}
        mock_post.return_value = mock_response
        
        self.parsed_model["config"]["packages"] = ["pandas", "numpy"]
        self.parsed_model["config"]["additional_libs"] = [{"maven": {"coordinates": "test:lib:1.0"}}]
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        helper._submit_job("/test/path", {"new_cluster": {}})
        
        call_args = mock_post.call_args
        libraries = call_args[1]["json"]["libraries"]
        self.assertEqual(len(libraries), 3)
        self.assertEqual(libraries[0], {"pypi": {"package": "pandas"}})
        self.assertEqual(libraries[1], {"pypi": {"package": "numpy"}})
        self.assertEqual(libraries[2], {"maven": {"coordinates": "test:lib:1.0"}})
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_submit_job_failure(self, mock_post):
        """Test _submit_job with failed response"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error"
        mock_post.return_value = mock_response
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper._submit_job("/test/path", {})
        self.assertIn("Error creating python run", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.time.sleep')
    def test_polling_success(self, mock_sleep):
        """Test polling with successful completion"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        # Mock status function that returns terminal state after 2 calls
        call_count = [0]
        def mock_status_func():
            call_count[0] += 1
            return {"state": "TERMINATED" if call_count[0] >= 2 else "RUNNING"}
        
        response = helper.polling(
            status_func=mock_status_func,
            status_func_kwargs={},
            get_state_func=lambda r: r["state"],
            terminal_states=("TERMINATED",),
            expected_end_state="TERMINATED",
            get_state_msg_func=lambda r: "Success"
        )
        
        self.assertEqual(response["state"], "TERMINATED")
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.time.sleep')
    @patch('dbt.adapters.watsonx_spark.python_submissions.time.time')
    def test_polling_timeout(self, mock_time, mock_sleep):
        """Test polling with timeout"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        # Mock time to simulate timeout
        mock_time.side_effect = [0, 0, 100000]  # Start, first check, timeout
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        helper.timeout = 10
        
        def mock_status_func():
            return {"state": "RUNNING"}
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper.polling(
                status_func=mock_status_func,
                status_func_kwargs={},
                get_state_func=lambda r: r["state"],
                terminal_states=("TERMINATED",),
                expected_end_state="TERMINATED",
                get_state_msg_func=lambda r: "Running"
            )
        self.assertIn("timed out", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.time.sleep')
    def test_polling_unexpected_end_state(self, mock_sleep):
        """Test polling with unexpected end state"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        helper = ConcreteHelper(self.parsed_model, self.credentials)
        
        def mock_status_func():
            return {"state": "ERROR"}
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper.polling(
                status_func=mock_status_func,
                status_func_kwargs={},
                get_state_func=lambda r: r["state"],
                terminal_states=("TERMINATED", "ERROR"),
                expected_end_state="TERMINATED",
                get_state_msg_func=lambda r: "Error occurred"
            )
        self.assertIn("ERROR", str(cm.exception))


class TestJobClusterPythonJobHelper(unittest.TestCase):
    """Test the JobClusterPythonJobHelper class"""
    
    def test_check_credentials_missing_config(self):
        """Test check_credentials raises error when job_cluster_config is missing"""
        credentials = MockCredentials()
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {}
        }
        
        with self.assertRaises(ValueError) as cm:
            JobClusterPythonJobHelper(parsed_model, credentials)
        self.assertIn("job_cluster_config is required", str(cm.exception))
    
    def test_check_credentials_with_config(self):
        """Test check_credentials passes when job_cluster_config is present"""
        credentials = MockCredentials()
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {"job_cluster_config": {"spark_version": "11.3.x"}}
        }
        
        # Should not raise
        helper = JobClusterPythonJobHelper(parsed_model, credentials)
        self.assertIsNotNone(helper)
    
    @patch.object(JobClusterPythonJobHelper, '_submit_through_notebook')
    def test_submit(self, mock_submit):
        """Test submit method"""
        credentials = MockCredentials()
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {"job_cluster_config": {"spark_version": "11.3.x"}}
        }
        
        helper = JobClusterPythonJobHelper(parsed_model, credentials)
        helper.submit("print('test')")
        
        mock_submit.assert_called_once()
        call_args = mock_submit.call_args
        self.assertEqual(call_args[0][0], "print('test')")
        self.assertIn("new_cluster", call_args[0][1])


class TestDBContext(unittest.TestCase):
    """Test the DBContext class"""
    
    def test_init(self):
        """Test DBContext initialization"""
        credentials = MockCredentials()
        auth_header = {"Authorization": "Bearer token"}
        
        context = DBContext(credentials, "cluster_123", auth_header)
        
        self.assertEqual(context.cluster_id, "cluster_123")
        self.assertEqual(context.host, "test.databricks.com")
        self.assertEqual(context.auth_header, auth_header)
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_create_success(self, mock_post):
        """Test create method with successful response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "context_123"}
        mock_post.return_value = mock_response
        
        credentials = MockCredentials()
        context = DBContext(credentials, "cluster_123", {})
        context_id = context.create()
        
        self.assertEqual(context_id, "context_123")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("/contexts/create", call_args[0][0])
        self.assertEqual(call_args[1]["json"]["clusterId"], "cluster_123")
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_create_failure(self, mock_post):
        """Test create method with failed response"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error"
        mock_post.return_value = mock_response
        
        credentials = MockCredentials()
        context = DBContext(credentials, "cluster_123", {})
        
        with self.assertRaises(DbtRuntimeError) as cm:
            context.create()
        self.assertIn("Error creating an execution context", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_destroy_success(self, mock_post):
        """Test destroy method with successful response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "context_123"}
        mock_post.return_value = mock_response
        
        credentials = MockCredentials()
        context = DBContext(credentials, "cluster_123", {})
        result = context.destroy("context_123")
        
        self.assertEqual(result, "context_123")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("/contexts/destroy", call_args[0][0])
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_destroy_failure(self, mock_post):
        """Test destroy method with failed response"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error"
        mock_post.return_value = mock_response
        
        credentials = MockCredentials()
        context = DBContext(credentials, "cluster_123", {})
        
        with self.assertRaises(DbtRuntimeError) as cm:
            context.destroy("context_123")
        self.assertIn("Error deleting an execution context", str(cm.exception))


class TestDBCommand(unittest.TestCase):
    """Test the DBCommand class"""
    
    def test_init(self):
        """Test DBCommand initialization"""
        credentials = MockCredentials()
        auth_header = {"Authorization": "Bearer token"}
        
        command = DBCommand(credentials, "cluster_123", auth_header)
        
        self.assertEqual(command.cluster_id, "cluster_123")
        self.assertEqual(command.host, "test.databricks.com")
        self.assertEqual(command.auth_header, auth_header)
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_execute_success(self, mock_post):
        """Test execute method with successful response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "command_123"}
        mock_post.return_value = mock_response
        
        credentials = MockCredentials()
        command = DBCommand(credentials, "cluster_123", {})
        command_id = command.execute("context_123", "print('test')")
        
        self.assertEqual(command_id, "command_123")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("/commands/execute", call_args[0][0])
        self.assertEqual(call_args[1]["json"]["command"], "print('test')")
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.post')
    def test_execute_failure(self, mock_post):
        """Test execute method with failed response"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error"
        mock_post.return_value = mock_response
        
        credentials = MockCredentials()
        command = DBCommand(credentials, "cluster_123", {})
        
        with self.assertRaises(DbtRuntimeError) as cm:
            command.execute("context_123", "code")
        self.assertIn("Error creating a command", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.get')
    def test_status_success(self, mock_get):
        """Test status method with successful response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "Finished", "results": {}}
        mock_get.return_value = mock_response
        
        credentials = MockCredentials()
        command = DBCommand(credentials, "cluster_123", {})
        status = command.status("context_123", "command_123")
        
        self.assertEqual(status["status"], "Finished")
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        self.assertIn("/commands/status", call_args[0][0])
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.get')
    def test_status_failure(self, mock_get):
        """Test status method with failed response"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Error"
        mock_get.return_value = mock_response
        
        credentials = MockCredentials()
        command = DBCommand(credentials, "cluster_123", {})
        
        with self.assertRaises(DbtRuntimeError) as cm:
            command.status("context_123", "command_123")
        self.assertIn("Error getting status of command", str(cm.exception))


class TestAllPurposeClusterPythonJobHelper(unittest.TestCase):
    """Test the AllPurposeClusterPythonJobHelper class"""
    
    def test_check_credentials_missing_cluster_id(self):
        """Test check_credentials raises error when cluster_id is missing"""
        credentials = MockCredentials(cluster_id=None)
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {}
        }
        
        with self.assertRaises(ValueError) as cm:
            AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        self.assertIn("cluster_id is required", str(cm.exception))
    
    def test_check_credentials_with_cluster_id(self):
        """Test check_credentials passes when cluster_id is present"""
        credentials = MockCredentials(cluster_id="cluster_123")
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {}
        }
        
        # Should not raise
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        self.assertIsNotNone(helper)
    
    @patch.object(AllPurposeClusterPythonJobHelper, '_submit_through_notebook')
    def test_submit_with_notebook(self, mock_submit):
        """Test submit method with create_notebook=True"""
        credentials = MockCredentials(cluster_id="cluster_123")
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {"create_notebook": True}
        }
        
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        helper.submit("print('test')")
        
        mock_submit.assert_called_once()
        call_args = mock_submit.call_args
        self.assertEqual(call_args[0][0], "print('test')")
        self.assertIn("existing_cluster_id", call_args[0][1])
    @patch.object(DBContext, 'destroy')
    @patch.object(DBContext, 'create')
    @patch.object(DBCommand, 'status')
    @patch.object(DBCommand, 'execute')
    @patch.object(AllPurposeClusterPythonJobHelper, 'polling')
    def test_submit_without_notebook_success(self, mock_polling, mock_execute, mock_status, mock_create, mock_destroy):
        """Test submit method without notebook (using commands)"""
        credentials = MockCredentials(cluster_id="cluster_123")
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {"create_notebook": False}
        }
        
        mock_create.return_value = "context_123"
        mock_execute.return_value = "command_123"
        mock_polling.return_value = {
            "status": "Finished",
            "results": {"resultType": "text", "data": "Success"}
        }
        
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        helper.submit("print('test')")
        
        mock_create.assert_called_once()
        mock_execute.assert_called_once_with("context_123", "print('test')")
        mock_polling.assert_called_once()
        mock_destroy.assert_called_once_with("context_123")
    
    @patch.object(DBContext, 'destroy')
    @patch.object(DBContext, 'create')
    @patch.object(DBCommand, 'execute')
    @patch.object(AllPurposeClusterPythonJobHelper, 'polling')
    def test_submit_without_notebook_error(self, mock_polling, mock_execute, mock_create, mock_destroy):
        """Test submit method without notebook with error result"""
        credentials = MockCredentials(cluster_id="cluster_123")
        parsed_model = {
            "alias": "test",
            "schema": "test",
            "config": {"create_notebook": False}
        }
        
        mock_create.return_value = "context_123"
        mock_execute.return_value = "command_123"
        mock_polling.return_value = {
            "status": "Finished",
            "results": {"resultType": "error", "cause": "Division by zero"}
        }
        
        helper = AllPurposeClusterPythonJobHelper(parsed_model, credentials)
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper.submit("print(1/0)")
        self.assertIn("Division by zero", str(cm.exception))
        
        # Verify context was destroyed even after error
        mock_destroy.assert_called_once_with("context_123")


class TestSubmitThroughNotebook(unittest.TestCase):
    """Test the _submit_through_notebook method"""
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.get')
    @patch.object(BaseDatabricksHelper, 'polling')
    @patch.object(BaseDatabricksHelper, '_submit_job')
    @patch.object(BaseDatabricksHelper, '_upload_notebook')
    @patch.object(BaseDatabricksHelper, '_create_work_dir')
    def test_submit_through_notebook_success(self, mock_create_dir, mock_upload, mock_submit_job, mock_polling, mock_get):
        """Test _submit_through_notebook with successful execution"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        credentials = MockCredentials()
        parsed_model = {
            "alias": "test_model",
            "schema": "test_schema",
            "config": {}
        }
        
        mock_submit_job.return_value = "run_123"
        mock_polling.return_value = {"state": {"life_cycle_state": "TERMINATED"}}
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "metadata": {"state": {"result_state": "SUCCESS"}},
            "error_trace": ""
        }
        mock_get.return_value = mock_response
        
        helper = ConcreteHelper(parsed_model, credentials)
        helper._submit_through_notebook("print('test')", {"new_cluster": {}})
        
        mock_create_dir.assert_called_once_with("/Shared/dbt_python_model/test_schema/")
        mock_upload.assert_called_once_with("/Shared/dbt_python_model/test_schema/test_model", "print('test')")
        mock_submit_job.assert_called_once()
        mock_polling.assert_called_once()
        mock_get.assert_called_once()
    
    @patch('dbt.adapters.watsonx_spark.python_submissions.requests.get')
    @patch.object(BaseDatabricksHelper, 'polling')
    @patch.object(BaseDatabricksHelper, '_submit_job')
    @patch.object(BaseDatabricksHelper, '_upload_notebook')
    @patch.object(BaseDatabricksHelper, '_create_work_dir')
    def test_submit_through_notebook_failure(self, mock_create_dir, mock_upload, mock_submit_job, mock_polling, mock_get):
        """Test _submit_through_notebook with failed execution"""
        class ConcreteHelper(BaseDatabricksHelper):
            def check_credentials(self):
                pass
            def submit(self, compiled_code):
                pass
        
        credentials = MockCredentials()
        parsed_model = {
            "alias": "test_model",
            "schema": "test_schema",
            "config": {}
        }
        
        mock_submit_job.return_value = "run_123"
        mock_polling.return_value = {"state": {"life_cycle_state": "TERMINATED"}}
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "metadata": {"state": {"result_state": "FAILED"}},
            "error_trace": "Traceback: Error in line 5"
        }
        mock_get.return_value = mock_response
        
        helper = ConcreteHelper(parsed_model, credentials)
        
        with self.assertRaises(DbtRuntimeError) as cm:
            helper._submit_through_notebook("print('test')", {"new_cluster": {}})
        self.assertIn("Python model failed", str(cm.exception))
        self.assertIn("Traceback", str(cm.exception))



if __name__ == '__main__':
    unittest.main()

# Made with Bob