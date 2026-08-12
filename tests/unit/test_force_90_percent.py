"""Aggressive tests to force 90% coverage - mocking all difficult paths"""
import pytest
from unittest import mock
from multiprocessing import get_context
import sys

# Mock missing imports before importing the module
sys.modules['TCLIService'] = mock.MagicMock()
sys.modules['TCLIService.ttypes'] = mock.MagicMock()
sys.modules['thrift'] = mock.MagicMock()
sys.modules['thrift.transport'] = mock.MagicMock()
sys.modules['thrift.transport.THttpClient'] = mock.MagicMock()
sys.modules['pyhive'] = mock.MagicMock()
sys.modules['thrift.transport.TSSLSocket'] = mock.MagicMock()
sys.modules['thrift_sasl'] = mock.MagicMock()
sys.modules['puresasl'] = mock.MagicMock()
sys.modules['puresasl.client'] = mock.MagicMock()

from dbt.adapters.watsonx_spark.connections import (
    SparkCredentials, SparkConnectionMethod, SparkConnectionManager,
    PyhiveConnectionWrapper, _is_retryable_error
)
from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError


class TestSparkCredentialsImportErrors:
    """Test SparkCredentials with import errors"""
    
    def test_credentials_without_pyhive(self):
        """Test credentials when PyHive is not available"""
        # Temporarily set hive to None
        import dbt.adapters.watsonx_spark.connections as conn_module
        original_hive = conn_module.hive
        original_thrift = conn_module.ThriftState
        original_http = conn_module.THttpClient
        
        try:
            conn_module.hive = None
            conn_module.ThriftState = None
            conn_module.THttpClient = None
            
            with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                SparkCredentials(
                    method="http", host="localhost", schema="test", catalog="test",
                    token="token", organization="org", cluster="cluster"
                )
        finally:
            conn_module.hive = original_hive
            conn_module.ThriftState = original_thrift
            conn_module.THttpClient = original_http
    
    def test_credentials_without_pyodbc(self):
        """Test credentials when pyodbc is not available"""
        import dbt.adapters.watsonx_spark.connections as conn_module
        original_pyodbc = conn_module.pyodbc
        
        try:
            conn_module.pyodbc = None
            
            with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                SparkCredentials(
                    method="odbc", host="localhost", schema="test", catalog="test",
                    driver="Simba", token="token"
                )
        finally:
            conn_module.pyodbc = original_pyodbc
    
    def test_credentials_session_without_pyspark(self):
        """Test credentials when pyspark is not available"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator"):
            with mock.patch.dict('sys.modules', {'pyspark': None}):
                with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                    SparkCredentials(
                        method="session", host="localhost", schema="test", catalog="test"
                    )


class TestPyhiveConnectionWrapperAbstractMethods:
    """Test PyhiveConnectionWrapper abstract method coverage"""
    
    def test_cursor_method(self):
        """Test cursor method"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        result = wrapper.cursor()
        assert result == wrapper
    
    def test_cancel_method(self):
        """Test cancel method"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock.MagicMock()
        wrapper.cancel()  # Should not raise
    
    def test_cancel_with_error(self):
        """Test cancel method with EnvironmentError"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        mock_cursor.cancel.side_effect = EnvironmentError("Cancel failed")
        wrapper._cursor = mock_cursor
        wrapper.cancel()  # Should catch and log error
    
    def test_close_method(self):
        """Test close method"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock.MagicMock()
        wrapper.close()
        assert wrapper.handle.close.called
    
    def test_close_with_error(self):
        """Test close method with EnvironmentError"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        mock_cursor.close.side_effect = EnvironmentError("Close failed")
        wrapper._cursor = mock_cursor
        wrapper.close()  # Should catch and log error
    
    def test_rollback_method(self):
        """Test rollback method"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper.rollback()  # Should just log
    
    def test_fetchall_method(self):
        """Test fetchall method"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [["result"]]
        wrapper._cursor = mock_cursor
        result = wrapper.fetchall()
        assert result == [["result"]]
    
    def test_description_property(self):
        """Test description property"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        mock_cursor.description = [("col1", "string", None, None, None, None, True)]
        wrapper._cursor = mock_cursor
        result = wrapper.description
        assert len(result) == 1


class TestPyhiveExecutePolling:
    """Test PyhiveConnectionWrapper execute with polling states"""
    
    def test_execute_with_error_message(self):
        """Test execute when poll returns error message"""
        from TCLIService.ttypes import TOperationState as ThriftState
        
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        wrapper._cursor = mock_cursor
        
        # Mock poll to return error
        mock_poll_state = mock.MagicMock()
        mock_poll_state.operationState = ThriftState.FINISHED_STATE
        mock_poll_state.errorMessage = "Query failed with error"
        mock_cursor.poll.return_value = mock_poll_state
        
        with mock.patch("time.sleep"):
            with pytest.raises(DbtDatabaseError, match="Query failed with error"):
                wrapper.execute("SELECT 1")
    
    def test_execute_with_cancelled_state(self):
        """Test execute when query is cancelled"""
        from TCLIService.ttypes import TOperationState as ThriftState
        
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        wrapper._cursor = mock_cursor
        
        # Mock poll to return cancelled state
        mock_poll_state = mock.MagicMock()
        mock_poll_state.operationState = ThriftState.CANCELED_STATE
        mock_poll_state.errorMessage = None
        mock_cursor.poll.return_value = mock_poll_state
        
        with mock.patch("time.sleep"):
            with pytest.raises(DbtDatabaseError, match="CANCELED_STATE"):
                wrapper.execute("SELECT 1")
    
    def test_execute_with_pending_then_success(self):
        """Test execute with pending state then success"""
        from TCLIService.ttypes import TOperationState as ThriftState
        
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        mock_cursor = mock.MagicMock()
        wrapper._cursor = mock_cursor
        
        # Mock poll to return pending then success
        call_count = [0]
        def mock_poll():
            call_count[0] += 1
            mock_state = mock.MagicMock()
            if call_count[0] == 1:
                mock_state.operationState = ThriftState.PENDING_STATE
            else:
                mock_state.operationState = ThriftState.FINISHED_STATE
            mock_state.errorMessage = None
            return mock_state
        
        mock_cursor.poll = mock_poll
        
        with mock.patch("time.sleep"):
            wrapper.execute("SELECT 1")  # Should succeed after pending


class TestIsRetryableErrorPatterns:
    """Test _is_retryable_error with all patterns"""
    
    def test_pending_pattern(self):
        """Test pending pattern"""
        exc = Exception("Operation is pending")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_temporarily_unavailable_pattern(self):
        """Test temporarily_unavailable pattern"""
        exc = Exception("Resource temporarily_unavailable")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_connection_reset_pattern(self):
        """Test connection reset pattern"""
        exc = Exception("Connection reset by peer")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_connection_refused_pattern(self):
        """Test connection refused pattern"""
        exc = Exception("Connection refused")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_too_many_connections_pattern(self):
        """Test too many connections pattern"""
        exc = Exception("Too many connections")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_service_unavailable_pattern(self):
        """Test service unavailable pattern"""
        exc = Exception("Service unavailable")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_server_busy_pattern(self):
        """Test server is busy pattern"""
        exc = Exception("Server is busy, please try again")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_throttled_pattern(self):
        """Test throttled pattern"""
        exc = Exception("Request throttled")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_http_429_pattern(self):
        """Test HTTP 429 pattern"""
        exc = Exception("HTTP 429 Too Many Requests")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_http_503_pattern(self):
        """Test HTTP 503 pattern"""
        exc = Exception("HTTP 503 Service Unavailable")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_http_504_pattern(self):
        """Test HTTP 504 pattern"""
        exc = Exception("HTTP 504 Gateway Timeout")
        result = _is_retryable_error(exc)
        assert "Retryable" in result
    
    def test_gateway_timeout_phrase(self):
        """Test gateway timeout phrase"""
        exc = Exception("Gateway timeout occurred")
        result = _is_retryable_error(exc)
        assert "Retryable" in result


class TestConnectionManagerExceptionHandlerPaths:
    """Test exception_handler with various exception types"""
    
    def test_exception_handler_with_string_error(self):
        """Test exception_handler with string error message"""
        from dbt.adapters.watsonx_spark.http_auth.exceptions import TokenRetrievalError
        
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        with pytest.raises(DbtRuntimeError, match="Authentication token error"):
            with manager.exception_handler("SELECT 1"):
                raise TokenRetrievalError("Token expired")
    
    def test_exception_handler_with_invalid_credentials(self):
        """Test exception_handler with InvalidCredentialsError"""
        from dbt.adapters.watsonx_spark.http_auth.exceptions import InvalidCredentialsError
        
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        with pytest.raises(DbtRuntimeError, match="Authentication failed"):
            with manager.exception_handler("SELECT 1"):
                raise InvalidCredentialsError("Invalid credentials")
    
    def test_exception_handler_with_catalog_error(self):
        """Test exception_handler with CatalogDetailsError"""
        from dbt.adapters.watsonx_spark.http_auth.exceptions import CatalogDetailsError
        
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        with pytest.raises(DbtRuntimeError, match="Catalog error"):
            with manager.exception_handler("SELECT 1"):
                raise CatalogDetailsError("test_catalog", "Catalog not found")
    
    def test_exception_handler_with_v2_table_error(self):
        """Test exception_handler with v2 table error"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        exc = Exception("SHOW TABLE EXTENDED is not supported for v2 tables")
        
        with pytest.raises(DbtRuntimeError):
            with manager.exception_handler("SHOW TABLE EXTENDED"):
                raise exc
    
    def test_exception_handler_with_schema_not_found(self):
        """Test exception_handler with schema_not_found error"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        exc = Exception("schema_not_found: Schema does not exist")
        
        with pytest.raises(DbtRuntimeError):
            with manager.exception_handler("USE SCHEMA test"):
                raise exc
    
    def test_exception_handler_with_permission_denied(self):
        """Test exception_handler with permission denied"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        mock_exc = Exception()
        mock_status = mock.MagicMock()
        mock_status.errorMessage = "Permission denied for table"
        mock_exc.args = [mock.MagicMock(status=mock_status)]
        
        with pytest.raises(DbtRuntimeError, match="Permission denied"):
            with manager.exception_handler("SELECT * FROM table"):
                raise mock_exc
    
    def test_exception_handler_with_table_not_found(self):
        """Test exception_handler with table not found"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        mock_exc = Exception()
        mock_status = mock.MagicMock()
        mock_status.errorMessage = "Table not found: my_table"
        mock_exc.args = [mock.MagicMock(status=mock_status)]
        
        with pytest.raises(DbtRuntimeError, match="Table not found"):
            with manager.exception_handler("SELECT * FROM my_table"):
                raise mock_exc
    
    def test_exception_handler_with_syntax_error(self):
        """Test exception_handler with syntax error"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        mock_exc = Exception()
        mock_status = mock.MagicMock()
        mock_status.errorMessage = "Syntax error in SQL statement"
        mock_exc.args = [mock.MagicMock(status=mock_status)]
        
        with pytest.raises(DbtRuntimeError, match="Syntax error"):
            with manager.exception_handler("SELCT * FROM table"):
                raise mock_exc
    
    def test_exception_handler_with_no_error_message(self):
        """Test exception_handler with no error message in thrift response"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        mock_exc = Exception()
        mock_exc.args = [mock.MagicMock()]  # No status attribute
        
        with pytest.raises(DbtRuntimeError):
            with manager.exception_handler("SELECT 1"):
                raise mock_exc
    
    def test_exception_handler_with_no_args(self):
        """Test exception_handler with exception that has no args"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        exc = Exception()
        exc.args = []
        
        with pytest.raises(Exception):
            with manager.exception_handler("SELECT 1"):
                raise exc


class TestConnectionManagerNoOpMethods:
    """Test connection manager no-op methods"""
    
    def test_add_begin_query(self):
        """Test add_begin_query is no-op"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        manager.add_begin_query()  # Should just log
    
    def test_add_commit_query(self):
        """Test add_commit_query is no-op"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        manager.add_commit_query()  # Should just log
    
    def test_commit(self):
        """Test commit is no-op"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        manager.commit()  # Should just log
    
    def test_rollback(self):
        """Test rollback is no-op"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        manager.rollback()  # Should just log


class TestConnectionOpenAlreadyOpen:
    """Test connection open when already open"""
    
    def test_open_already_open_connection(self):
        """Test open skips when connection already open"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        mock_connection = mock.MagicMock()
        mock_connection.state = "open"
        
        result = manager.open(mock_connection)
        assert result == mock_connection


class TestConnectionOpenWithUri:
    """Test connection open with custom URI"""
    
    def test_open_http_with_custom_uri(self):
        """Test open HTTP connection with custom URI"""
        from dbt.adapters.watsonx_spark.http_auth.authenticator import get_authenticator
        
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        mock_connection = mock.MagicMock()
        mock_connection.state = "closed"
        mock_connection.credentials = SparkCredentials(
            method="http", host="https://localhost", schema="test", catalog="test",
            token="token", organization="org", cluster="cluster",
            uri="/custom/path"
        )
        
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator"):
            with mock.patch("dbt.adapters.watsonx_spark.connections.hive.connect") as mock_connect:
                with mock.patch("dbt.adapters.watsonx_spark.connections.THttpClient"):
                    mock_connect.return_value = mock.MagicMock()
                    
                    connection = manager.open(mock_connection)
                    assert connection.state == "open"


class TestDataTypeCodeToName:
    """Test data_type_code_to_name"""
    
    def test_data_type_code_with_string(self):
        """Test with string type code"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        result = manager.data_type_code_to_name("VARCHAR")
        assert result == "VARCHAR"
    
    def test_data_type_code_with_type(self):
        """Test with Python type"""
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        result = manager.data_type_code_to_name(int)
        assert result == "INT"

# Made with Bob
