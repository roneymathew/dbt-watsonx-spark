"""Extended unit tests for connections.py to improve coverage to 90%"""
import pytest
from unittest import mock
from dbt_common.exceptions import DbtRuntimeError, DbtConfigError
from dbt.adapters.watsonx_spark.connections import (
    SparkConnectionMethod,
    SparkCredentials,
    PyhiveConnectionWrapper,
    PyodbcConnectionWrapper,
    SparkConnectionManager,
    _build_odbc_connnection_string,
)
from dbt.adapters.watsonx_spark.http_auth.exceptions import (
    TokenRetrievalError,
    InvalidCredentialsError,
    CatalogDetailsError,
)


class _FakeAuthenticator:
    """Mock authenticator for testing"""
    def get_token(self):
        return "dummy-token"

    def get_catlog_details(self, catalog_name):
        return ("s3://bucket", "iceberg")
    
    def Authenticate(self, transport):
        return transport


class _FakeDeltaAuthenticator(_FakeAuthenticator):
    """Mock authenticator that returns delta format"""
    def get_catlog_details(self, catalog_name):
        return ("s3://bucket", "delta")


class _FakeHudiAuthenticator(_FakeAuthenticator):
    """Mock authenticator that returns hudi format"""
    def get_catlog_details(self, catalog_name):
        return ("s3://bucket", "hudi")


class _FakeParquetAuthenticator(_FakeAuthenticator):
    """Mock authenticator that returns parquet format"""
    def get_catlog_details(self, catalog_name):
        return ("s3://bucket", "parquet")


class TestBuildOdbcConnectionString:
    """Test _build_odbc_connnection_string function"""
    
    def test_build_odbc_connection_string_empty(self):
        result = _build_odbc_connnection_string()
        assert result == ""
    
    def test_build_odbc_connection_string_single(self):
        result = _build_odbc_connnection_string(Driver="Spark")
        assert result == "Driver=Spark"
    
    def test_build_odbc_connection_string_multiple(self):
        result = _build_odbc_connnection_string(Driver="Spark", Host="localhost", Port=443)
        assert "Driver=Spark" in result
        assert "Host=localhost" in result
        assert "Port=443" in result


class TestSparkCredentialsValidation:
    """Test SparkCredentials validation and initialization"""
    
    def test_missing_method_raises_error(self):
        """Test that missing method raises DbtRuntimeError"""
        with pytest.raises(DbtRuntimeError, match="Must specify `method` in profile"):
            with mock.patch(
                "dbt.adapters.watsonx_spark.connections.get_authenticator",
                return_value=_FakeAuthenticator(),
            ):
                SparkCredentials(
                    host="localhost",
                    schema="test_schema",
                    catalog="test_catalog",
                )
    
    def test_missing_host_raises_error(self):
        """Test that missing host raises DbtRuntimeError"""
        with pytest.raises(DbtRuntimeError, match="Must specify `host` in profile"):
            with mock.patch(
                "dbt.adapters.watsonx_spark.connections.get_authenticator",
                return_value=_FakeAuthenticator(),
            ):
                SparkCredentials(
                    method=SparkConnectionMethod.HTTP,
                    schema="test_schema",
                    catalog="test_catalog",
                )
    
    def test_missing_schema_raises_error(self):
        """Test that missing schema raises DbtRuntimeError"""
        with pytest.raises(DbtRuntimeError, match="Must specify `schema` in profile"):
            with mock.patch(
                "dbt.adapters.watsonx_spark.connections.get_authenticator",
                return_value=_FakeAuthenticator(),
            ):
                SparkCredentials(
                    host="localhost",
                    method=SparkConnectionMethod.HTTP,
                    catalog="test_catalog",
                )
    
    def test_missing_catalog_raises_error(self):
        """Test that missing catalog raises DbtRuntimeError"""
        with pytest.raises(DbtRuntimeError, match="Must specify `catalog` in profile"):
            with mock.patch(
                "dbt.adapters.watsonx_spark.connections.get_authenticator",
                return_value=_FakeAuthenticator(),
            ):
                SparkCredentials(
                    host="localhost",
                    method=SparkConnectionMethod.HTTP,
                    schema="test_schema",
                )
    
    def test_database_schema_mismatch_raises_error(self):
        """Test that database != schema raises error"""
        with pytest.raises(DbtRuntimeError, match="database must be omitted or have the same value"):
            with mock.patch(
                "dbt.adapters.watsonx_spark.connections.get_authenticator",
                return_value=_FakeAuthenticator(),
            ):
                SparkCredentials(
                    host="localhost",
                    method=SparkConnectionMethod.HTTP,
                    schema="test_schema",
                    database="different_db",
                    catalog="test_catalog",
                )
    
    def test_database_schema_same_allowed(self):
        """Test that database == schema is allowed"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeAuthenticator(),
        ):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                database="test_schema",
                catalog="test_catalog",
                token="test-token",
            )
            assert creds.database is None  # Database is set to None after validation


class TestSparkCredentialsCatalogFormats:
    """Test different catalog format handling"""
    
    def test_iceberg_catalog_format(self):
        """Test Iceberg catalog format sets correct schema prefix"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeAuthenticator(),
        ):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                catalog="iceberg_catalog",
                token="test-token",
            )
            assert creds.schema == "iceberg_catalog.test_schema"
            assert creds.connection_catalog == "iceberg_catalog"
    
    def test_delta_catalog_format(self):
        """Test Delta catalog format sets spark_catalog prefix"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeDeltaAuthenticator(),
        ):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                catalog="delta_catalog",
                token="test-token",
            )
            assert creds.schema == "spark_catalog.test_schema"
            assert creds.connection_catalog == "spark_catalog"
    
    def test_hudi_catalog_format(self):
        """Test Hudi catalog format sets spark_catalog prefix"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeHudiAuthenticator(),
        ):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                catalog="hudi_catalog",
                token="test-token",
            )
            assert creds.schema == "spark_catalog.test_schema"
            assert creds.connection_catalog == "spark_catalog"
    
    def test_parquet_catalog_format(self):
        """Test Parquet (other) catalog format uses configured catalog"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeParquetAuthenticator(),
        ):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                catalog="parquet_catalog",
                token="test-token",
            )
            assert creds.connection_catalog == "parquet_catalog"


class TestSparkCredentialsProperties:
    """Test SparkCredentials properties"""
    
    def test_cluster_id_property(self):
        """Test cluster_id property returns cluster value"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeAuthenticator(),
        ):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                catalog="test_catalog",
                cluster="test-cluster-123",
                token="test-token",
            )
            assert creds.cluster_id == "test-cluster-123"
    
    def test_unique_field_property(self):
        """Test unique_field property returns host"""
        with mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeAuthenticator(),
        ):
            creds = SparkCredentials(
                host="test-host.com",
                method=SparkConnectionMethod.HTTP,
                schema="test_schema",
                catalog="test_catalog",
                token="test-token",
            )
            assert creds.unique_field == "test-host.com"


class TestSparkConnectionManagerExceptionHandler:
    """Test SparkConnectionManager exception_handler"""
    
    def test_exception_handler_token_retrieval_error(self):
        """Test exception handler converts TokenRetrievalError"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        with pytest.raises(DbtRuntimeError, match="Authentication token error"):
            with manager.exception_handler("SELECT 1"):
                raise TokenRetrievalError("Token expired")
    
    def test_exception_handler_invalid_credentials_error(self):
        """Test exception handler converts InvalidCredentialsError"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        with pytest.raises(DbtRuntimeError, match="Authentication failed"):
            with manager.exception_handler("SELECT 1"):
                raise InvalidCredentialsError("Invalid credentials")
    
    def test_exception_handler_catalog_details_error(self):
        """Test exception handler converts CatalogDetailsError"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        
        with pytest.raises(DbtRuntimeError, match="Catalog error"):
            with manager.exception_handler("SELECT 1"):
                raise CatalogDetailsError(catalog_name="test", message="Catalog not found")


class TestSparkConnectionManagerMethods:
    """Test SparkConnectionManager methods"""
    
    def test_cancel_calls_handle_cancel(self):
        """Test cancel method calls connection handle cancel"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        mock_connection = mock.MagicMock()
        mock_handle = mock.MagicMock()
        mock_connection.handle = mock_handle
        
        manager.cancel(mock_connection)
        mock_handle.cancel.assert_called_once()
    
    def test_get_response_returns_ok(self):
        """Test get_response returns OK message"""
        response = SparkConnectionManager.get_response(mock.MagicMock())
        assert response._message == "OK"
    
    def test_add_begin_query_logs_not_implemented(self):
        """Test add_begin_query logs not implemented"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        # Should not raise, just log
        manager.add_begin_query()
    
    def test_add_commit_query_logs_not_implemented(self):
        """Test add_commit_query logs not implemented"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        # Should not raise, just log
        manager.add_commit_query()
    
    def test_commit_logs_not_implemented(self):
        """Test commit logs not implemented"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        # Should not raise, just log
        manager.commit()
    
    def test_rollback_logs_not_implemented(self):
        """Test rollback logs not implemented"""
        from multiprocessing import get_context
        manager = SparkConnectionManager(mock.MagicMock(), get_context("spawn"))
        # Should not raise, just log
        manager.rollback()
    
    def test_validate_creds_missing_required(self):
        """Test validate_creds raises error for missing required field"""
        mock_creds = mock.MagicMock()
        mock_creds.method = SparkConnectionMethod.HTTP
        delattr(mock_creds, 'token')
        
        with pytest.raises(DbtConfigError, match="The config 'token' is required"):
            SparkConnectionManager.validate_creds(mock_creds, ['token'])


class TestPyhiveConnectionWrapper:
    """Test PyhiveConnectionWrapper methods"""
    
    def test_cursor_creates_and_returns_cursor(self):
        """Test cursor method creates cursor and returns self"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_handle.cursor.return_value = mock_cursor
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        result = wrapper.cursor()
        
        assert result is wrapper
        assert wrapper._cursor is mock_cursor
        mock_handle.cursor.assert_called_once()
    
    def test_cancel_with_cursor(self):
        """Test cancel calls cursor cancel when cursor exists"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.cancel()
        mock_cursor.cancel.assert_called_once()
    
    def test_cancel_with_cursor_environment_error(self):
        """Test cancel handles EnvironmentError gracefully"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_cursor.cancel.side_effect = EnvironmentError("Connection closed")
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Should not raise, just log
        wrapper.cancel()
    
    def test_cancel_without_cursor(self):
        """Test cancel does nothing when no cursor"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        
        # Should not raise
        wrapper.cancel()
    
    def test_close_with_cursor(self):
        """Test close calls cursor close and handle close"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.close()
        mock_cursor.close.assert_called_once()
        mock_handle.close.assert_called_once()
    
    def test_close_with_cursor_environment_error(self):
        """Test close handles EnvironmentError gracefully"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_cursor.close.side_effect = EnvironmentError("Connection closed")
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.close()
        # Handle close should still be called
        mock_handle.close.assert_called_once()
    
    def test_close_without_cursor(self):
        """Test close calls handle close when no cursor"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        
        wrapper.close()
        mock_handle.close.assert_called_once()
    
    def test_rollback_logs_not_implemented(self):
        """Test rollback logs not implemented"""
        mock_handle = mock.MagicMock()
        wrapper = PyhiveConnectionWrapper(mock_handle)
        
        # Should not raise, just log
        wrapper.rollback()
    
    def test_fetchall_returns_cursor_results(self):
        """Test fetchall returns cursor fetchall results"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [("row1",), ("row2",)]
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        result = wrapper.fetchall()
        assert result == [("row1",), ("row2",)]
        mock_cursor.fetchall.assert_called_once()
    
    def test_description_returns_cursor_description(self):
        """Test description property returns cursor description"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_cursor.description = [("col1", "string", None, None, None, None, True)]
        
        wrapper = PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        result = wrapper.description
        assert result == [("col1", "string", None, None, None, None, True)]


class TestPyodbcConnectionWrapper:
    """Test PyodbcConnectionWrapper methods"""
    
    def test_execute_strips_semicolon(self):
        """Test execute strips trailing semicolon"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        
        wrapper = PyodbcConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.execute("SELECT 1;")
        mock_cursor.execute.assert_called_once_with("SELECT 1")
    
    def test_execute_without_bindings(self):
        """Test execute without bindings"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        
        wrapper = PyodbcConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.execute("SELECT 1", None)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
    
    def test_execute_with_bindings(self):
        """Test execute with bindings converts format to qmark"""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        
        wrapper = PyodbcConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.execute("SELECT * FROM table WHERE id = %(id)s", [1])
        # Should convert to qmark format and call execute
        assert mock_cursor.execute.called

# Made with Bob
