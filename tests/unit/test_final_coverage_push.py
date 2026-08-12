"""Final push to 90% coverage - focusing on impl.py and connections.py gaps"""
import pytest
from unittest import mock
from multiprocessing import get_context
from dbt.adapters.watsonx_spark import WatsonxSparkAdapter
from dbt.adapters.watsonx_spark.connections import SparkConnectionManager, SparkCredentials, SparkConnectionMethod
from dbt_common.exceptions import DbtRuntimeError
from .utils import config_from_parts_or_dicts


class _FakeAuthenticator:
    def get_token(self):
        return "dummy-token"
    def get_catlog_details(self, catalog_name):
        return ("", "parquet")
    def Authenticate(self, transport):
        return transport


@pytest.fixture
def adapter():
    """Create adapter instance"""
    with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
        project_cfg = {
            "name": "X", "version": "0.1", "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {"identifier": False, "schema": False},
            "config-version": 2,
        }
        profile = {
            "outputs": {
                "test": {
                    "type": "watsonx_spark", "method": "http",
                    "schema": "analytics", "host": "myorg.sparkhost.com",
                    "port": 443, "token": "abc123", "catalog": "spark_catalog",
                    "organization": "0123456789", "cluster": "01234-23423-coffeetime",
                }
            },
            "target": "test",
        }
        config = config_from_parts_or_dicts(project_cfg, profile)
        return WatsonxSparkAdapter(config, get_context("spawn"))


class TestImplAdapterMethods:
    """Test impl.py adapter methods"""
    
    def test_get_adapter_run_info(self, adapter):
        """Test get_adapter_run_info returns correct info"""
        result = adapter.get_adapter_run_info(mock.MagicMock())
        assert result.adapter_name == "watsonx_spark"
        assert result.adapter_version is not None
    
    def test_default_python_submission_method(self, adapter):
        """Test default_python_submission_method"""
        result = adapter.default_python_submission_method
        assert result == "job_cluster"
    
    def test_generate_python_submission_response(self, adapter):
        """Test generate_python_submission_response"""
        mock_submission_result = {"state": "SUCCESS"}
        result = adapter.generate_python_submission_response(mock_submission_result)
        assert result._message == "SUCCESS"


class TestConnectionsOdbcMethods:
    """Test ODBC-related methods in connections.py"""
    
    def test_odbc_credentials_validation(self):
        """Test ODBC method requires pyodbc"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            with mock.patch("dbt.adapters.watsonx_spark.connections.pyodbc", None):
                with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                    SparkCredentials(
                        host="localhost",
                        method=SparkConnectionMethod.ODBC,
                        schema="test",
                        catalog="test_catalog",
                    )
    
    def test_odbc_cluster_and_endpoint_conflict(self):
        """Test ODBC with both cluster and endpoint raises error"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            with pytest.raises(DbtRuntimeError, match="cannot both be set"):
                SparkCredentials(
                    host="localhost",
                    method=SparkConnectionMethod.ODBC,
                    schema="test",
                    catalog="test_catalog",
                    cluster="cluster1",
                    endpoint="endpoint1",
                    driver="ODBC",
                )
    
    def test_thrift_method_requires_pyhive(self):
        """Test THRIFT method requires PyHive"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            with mock.patch("dbt.adapters.watsonx_spark.connections.ThriftState", None):
                with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                    SparkCredentials(
                        host="localhost",
                        method=SparkConnectionMethod.THRIFT,
                        schema="test",
                        catalog="test_catalog",
                    )
    
    def test_http_method_requires_pyhive(self):
        """Test HTTP method requires PyHive"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            with mock.patch("dbt.adapters.watsonx_spark.connections.hive", None):
                with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                    SparkCredentials(
                        host="localhost",
                        method=SparkConnectionMethod.HTTP,
                        schema="test",
                        catalog="test_catalog",
                    )
    
    def test_session_method_requires_pyspark(self):
        """Test SESSION method requires pyspark"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            # Mock the import to fail
            import sys
            original_import = __builtins__.__import__
            
            def mock_import(name, *args, **kwargs):
                if name == "pyspark":
                    raise ImportError("No module named 'pyspark'")
                return original_import(name, *args, **kwargs)
            
            with mock.patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(DbtRuntimeError, match="additional dependencies"):
                    SparkCredentials(
                        host="localhost",
                        method=SparkConnectionMethod.SESSION,
                        schema="test",
                        catalog="test_catalog",
                    )
    
    def test_host_trailing_slash_removed(self):
        """Test that trailing slash is removed from host"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            creds = SparkCredentials(
                host="localhost/",
                method=SparkConnectionMethod.HTTP,
                schema="test",
                catalog="test_catalog",
                token="test",
            )
            assert creds.host == "localhost"
    
    def test_auth_dict_conversion(self):
        """Test auth dict values are converted to strings"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test",
                catalog="test_catalog",
                token="test",
                auth={"type": 123, "value": 456},
            )
            assert creds.auth["type"] == "123"
            assert creds.auth["value"] == "456"
    
    def test_auth_string_conversion(self):
        """Test auth string is converted to dict"""
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator", return_value=_FakeAuthenticator()):
            creds = SparkCredentials(
                host="localhost",
                method=SparkConnectionMethod.HTTP,
                schema="test",
                catalog="test_catalog",
                token="test",
                auth="KERBEROS",
            )
            assert creds.auth == {"type": "KERBEROS"}


class TestConnectionManagerMethods:
    """Test SparkConnectionManager additional methods"""
    
    def test_get_location_from_api_success(self):
        """Test get_location_from_api returns bucket and format"""
        mock_creds = mock.MagicMock()
        mock_creds.catalog = "test_catalog"
        mock_creds.auth = None
        mock_creds.host = "test.com"
        mock_creds.uri = None
        mock_creds.suppress_ssl_warnings = True
        
        with mock.patch("dbt.adapters.watsonx_spark.connections.get_authenticator") as mock_auth:
            mock_authenticator = mock.MagicMock()
            mock_authenticator.get_catlog_details.return_value = ("s3://bucket", "iceberg")
            mock_auth.return_value = mock_authenticator
            
            result = SparkConnectionManager.get_location_from_api(mock_creds)
            assert result == ("s3://bucket", "iceberg")
    
    def test_get_location_from_api_no_catalog(self):
        """Test get_location_from_api with no catalog returns None"""
        mock_creds = mock.MagicMock()
        mock_creds.catalog = None
        
        result = SparkConnectionManager.get_location_from_api(mock_creds)
        assert result is None


class TestImplHelperMethods:
    """Test impl.py helper methods"""
    
    def test_should_create_schema_true(self, adapter):
        """Test should_create_schema returns True when create_schemas is True"""
        mock_relation = mock.MagicMock()
        mock_relation.without_identifier.return_value = mock_relation
        
        with mock.patch.object(adapter, "_get_active_credentials") as mock_creds:
            mock_creds.return_value.create_schemas = True
            result = adapter.should_create_schema(mock_relation)
            assert result is True
    
    def test_should_create_schema_false(self, adapter):
        """Test should_create_schema returns False when create_schemas is False"""
        mock_relation = mock.MagicMock()
        mock_relation.without_identifier.return_value = mock_relation
        
        with mock.patch.object(adapter, "_get_active_credentials") as mock_creds:
            mock_creds.return_value.create_schemas = False
            result = adapter.should_create_schema(mock_relation)
            assert result is False
    
    def test_get_active_credentials(self, adapter):
        """Test _get_active_credentials returns credentials"""
        with mock.patch.object(adapter.connections, "get_thread_connection") as mock_conn:
            mock_creds = mock.MagicMock()
            mock_conn.return_value.credentials = mock_creds
            
            result = adapter._get_active_credentials()
            assert result == mock_creds
    
    def test_should_set_location_with_auto_location(self, adapter):
        """Test should_set_location with auto_location enabled"""
        mock_relation = mock.MagicMock()
        mock_config = {"file_format": "delta"}
        
        with mock.patch.object(adapter, "_get_active_credentials") as mock_creds:
            mock_creds.return_value.auto_location = True
            mock_creds.return_value.location_root = "/data"
            
            result = adapter.should_set_location(mock_relation, mock_config)
            assert result is True
    
    def test_should_set_location_without_auto_location(self, adapter):
        """Test should_set_location without auto_location"""
        mock_relation = mock.MagicMock()
        mock_config = {"file_format": "delta"}
        
        with mock.patch.object(adapter, "_get_active_credentials") as mock_creds:
            mock_creds.return_value.auto_location = False
            
            result = adapter.should_set_location(mock_relation, mock_config)
            assert result is False

# Made with Bob
