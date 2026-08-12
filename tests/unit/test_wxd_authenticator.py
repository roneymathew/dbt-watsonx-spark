import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning
import requests

from dbt.adapters.watsonx_spark.http_auth.wxd_authenticator import (
    WatsonxData,
    WatsonxDataEnv,
    Token,
    CPD,
    SAAS,
    USER_AGENT,
)
from dbt.adapters.watsonx_spark.http_auth.exceptions import (
    TokenRetrievalError,
    InvalidCredentialsError,
    CatalogDetailsError,
)


class TestWatsonxDataEnv(unittest.TestCase):
    """Test the WatsonxDataEnv class"""
    
    def test_init(self):
        """Test WatsonxDataEnv initialization"""
        env = WatsonxDataEnv("CPD", "/auth/endpoint", "HeaderKey")
        
        self.assertEqual(env.envType, "CPD")
        self.assertEqual(env.authEndpoint, "/auth/endpoint")
        self.assertEqual(env.authInstanceHeaderKey, "HeaderKey")


class TestToken(unittest.TestCase):
    """Test the Token class"""
    
    def test_init(self):
        """Test Token initialization"""
        token = Token("test_token_value")
        self.assertEqual(token.token, "test_token_value")


class TestWatsonxDataInit(unittest.TestCase):
    """Test WatsonxData initialization"""
    
    def test_init_with_uri_version(self):
        """Test initialization with URI containing version"""
        profile = {
            "type": "watsonx",
            "instance": "test-instance",
            "user": "test-user",
            "apikey": "test-key",
            "suppress_ssl_warnings": True
        }
        host = "https://test.host.com"
        uri = "/lakehouse/api/v3/catalogs"
        
        wxd = WatsonxData(profile, host, uri)
        
        self.assertEqual(wxd.profile, profile)
        self.assertEqual(wxd.type, "watsonx")
        self.assertEqual(wxd.instance, "test-instance")
        self.assertEqual(wxd.user, "test-user")
        self.assertEqual(wxd.apikey, "test-key")
        self.assertEqual(wxd.host, host)
        self.assertEqual(wxd.uri, uri)
        self.assertEqual(wxd.lakehouse_version, "v3")
    
    def test_init_without_uri(self):
        """Test initialization without URI (uses default version)"""
        profile = {"instance": "test", "suppress_ssl_warnings": True}
        
        wxd = WatsonxData(profile, "host", None)
        
        self.assertEqual(wxd.lakehouse_version, "v2")
        self.assertIsNone(wxd.uri)
    
    def test_init_with_suppress_ssl_true(self):
        """Test initialization with suppress_ssl_warnings=True"""
        profile = {"instance": "test", "suppress_ssl_warnings": True}
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            wxd = WatsonxData(profile, "host", None)
            # SSL warnings should be suppressed
    
    def test_init_with_suppress_ssl_false(self):
        """Test initialization with suppress_ssl_warnings=False"""
        profile = {"instance": "test", "suppress_ssl_warnings": False}
        
        wxd = WatsonxData(profile, "host", None)
        # Warnings should not be suppressed
    
    def test_init_default_suppress_ssl(self):
        """Test initialization with default suppress_ssl_warnings (True)"""
        profile = {"instance": "test"}
        
        wxd = WatsonxData(profile, "host", None)
        # Should default to True


class TestExtractVersionFromUri(unittest.TestCase):
    """Test the _extract_version_from_uri method"""
    
    def test_extract_v2(self):
        """Test extracting v2 from URI"""
        wxd = WatsonxData({"instance": "test"}, "host", None)
        version = wxd._extract_version_from_uri("/lakehouse/api/v2/catalogs")
        self.assertEqual(version, "v2")
    
    def test_extract_v3(self):
        """Test extracting v3 from URI"""
        wxd = WatsonxData({"instance": "test"}, "host", None)
        version = wxd._extract_version_from_uri("/api/v3/something")
        self.assertEqual(version, "v3")
    
    def test_extract_v3_1(self):
        """Test extracting v3.1 from URI"""
        wxd = WatsonxData({"instance": "test"}, "host", None)
        version = wxd._extract_version_from_uri("/api/v3.1/endpoint")
        self.assertEqual(version, "v3.1")
    
    def test_extract_no_version(self):
        """Test URI without version returns None"""
        wxd = WatsonxData({"instance": "test"}, "host", None)
        version = wxd._extract_version_from_uri("/some/path")
        self.assertIsNone(version)


class TestGetEnvironment(unittest.TestCase):
    """Test the _get_environment method"""
    
    def test_saas_environment(self):
        """Test SaaS environment detection (instance contains 'crn')"""
        profile = {"instance": "crn:v1:bluemix:public:watsonxdata"}
        wxd = WatsonxData(profile, "host", None)
        
        env = wxd._get_environment()
        
        self.assertEqual(env.envType, SAAS)
        self.assertIn("/lakehouse/api/", env.authEndpoint)
        self.assertEqual(env.authInstanceHeaderKey, "AuthInstanceId")
    
    def test_cpd_environment(self):
        """Test CPD environment detection (instance without 'crn')"""
        profile = {"instance": "test-instance-id"}
        wxd = WatsonxData(profile, "host", None)
        
        env = wxd._get_environment()
        
        self.assertEqual(env.envType, CPD)
        self.assertEqual(env.authEndpoint, "/icp4d-api/v1/authorize")
        self.assertEqual(env.authInstanceHeaderKey, "LhInstanceId")


class TestAuthenticate(unittest.TestCase):
    """Test the Authenticate method"""
    
    @patch.object(WatsonxData, '_get_headers')
    def test_authenticate_sets_headers(self, mock_get_headers):
        """Test that Authenticate sets custom headers on transport"""
        mock_get_headers.return_value = {"Authorization": "Bearer token"}
        
        profile = {"instance": "test"}
        wxd = WatsonxData(profile, "host", None)
        
        mock_transport = Mock()
        result = wxd.Authenticate(mock_transport)
        
        mock_transport.setCustomHeaders.assert_called_once_with({"Authorization": "Bearer token"})
        self.assertEqual(result, mock_transport)


class TestGetToken(unittest.TestCase):
    """Test the get_token method"""
    
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_token_success(self, mock_get_env, mock_get_token):
        """Test successful token retrieval"""
        mock_env = Mock()
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        token = wxd.get_token()
        
        self.assertEqual(token, "test_token")
        mock_get_token.assert_called_once_with(mock_env)
    
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_token_none_token(self, mock_get_env, mock_get_token):
        """Test token retrieval when None is returned"""
        mock_get_env.return_value = Mock()
        mock_get_token.return_value = None
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd.get_token()
        self.assertIn("Failed to retrieve authentication token", str(cm.exception))
    
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_token_invalid_token_object(self, mock_get_env, mock_get_token):
        """Test token retrieval when token object has no token attribute"""
        mock_get_env.return_value = Mock()
        mock_get_token.return_value = Mock(spec=[])  # No 'token' attribute
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        
        with self.assertRaises(TokenRetrievalError):
            wxd.get_token()


class TestGetCpdToken(unittest.TestCase):
    """Test the _get_cpd_token method"""
    
    @patch.object(WatsonxData, '_post_request')
    def test_get_cpd_token_success(self, mock_post):
        """Test successful CPD token retrieval"""
        mock_post.return_value = {"token": "cpd_token_value"}
        
        profile = {"user": "test_user", "apikey": "test_key", "instance": "test"}
        wxd = WatsonxData(profile, "https://host.com", None)
        env = WatsonxDataEnv(CPD, "/icp4d-api/v1/authorize", "LhInstanceId")
        
        token = wxd._get_cpd_token(env)
        
        self.assertEqual(token.token, "cpd_token_value")
        mock_post.assert_called_once_with(
            "https://host.com/icp4d-api/v1/authorize",
            data={"username": "test_user", "api_key": "test_key"}
        )
    
    @patch.object(WatsonxData, '_post_request')
    def test_get_cpd_token_no_token_in_response(self, mock_post):
        """Test CPD token retrieval with invalid response"""
        mock_post.return_value = {"error": "something"}
        
        profile = {"user": "test_user", "apikey": "test_key", "instance": "test"}
        wxd = WatsonxData(profile, "https://host.com", None)
        env = WatsonxDataEnv(CPD, "/icp4d-api/v1/authorize", "LhInstanceId")
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd._get_cpd_token(env)
        self.assertIn("Invalid response format", str(cm.exception))


class TestGetSassToken(unittest.TestCase):
    """Test the _get_sass_token method"""
    
    @patch.object(WatsonxData, '_post_request')
    def test_get_sass_token_success_with_user(self, mock_post):
        """Test successful SaaS token retrieval with user"""
        mock_post.return_value = {"access_token": "sass_token_value"}
        
        profile = {"user": "test_user", "apikey": "test_key", "instance": "crn:test"}
        wxd = WatsonxData(profile, "https://host.com", None)
        env = WatsonxDataEnv(SAAS, "/lakehouse/api/v2/auth/authenticate", "AuthInstanceId")
        
        token = wxd._get_sass_token(env)
        
        self.assertEqual(token.token, "sass_token_value")
        call_args = mock_post.call_args[1]['data']
        self.assertEqual(call_args['username'], "ibmlhapikey_test_user")
    
    @patch.object(WatsonxData, '_post_request')
    def test_get_sass_token_success_without_user(self, mock_post):
        """Test successful SaaS token retrieval without user"""
        mock_post.return_value = {"accessToken": "sass_token_value"}
        
        profile = {"user": None, "apikey": "test_key", "instance": "crn:test"}
        wxd = WatsonxData(profile, "https://host.com", None)
        env = WatsonxDataEnv(SAAS, "/lakehouse/api/v2/auth/authenticate", "AuthInstanceId")
        
        token = wxd._get_sass_token(env)
        
        self.assertEqual(token.token, "sass_token_value")
        call_args = mock_post.call_args[1]['data']
        self.assertEqual(call_args['username'], "ibmlhapikey")
    
    @patch.object(WatsonxData, '_post_request')
    def test_get_sass_token_no_response(self, mock_post):
        """Test SaaS token retrieval with no response"""
        mock_post.return_value = None
        
        profile = {"user": "test", "apikey": "key", "instance": "crn:test"}
        wxd = WatsonxData(profile, "https://host.com", None)
        env = WatsonxDataEnv(SAAS, "/lakehouse/api/v2/auth/authenticate", "AuthInstanceId")
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd._get_sass_token(env)
        self.assertIn("Invalid response", str(cm.exception))
    
    @patch.object(WatsonxData, '_post_request')
    def test_get_sass_token_no_token_in_response(self, mock_post):
        """Test SaaS token retrieval with no token in response"""
        mock_post.return_value = {"error": "something"}
        
        profile = {"user": "test", "apikey": "key", "instance": "crn:test"}
        wxd = WatsonxData(profile, "https://host.com", None)
        env = WatsonxDataEnv(SAAS, "/lakehouse/api/v2/auth/authenticate", "AuthInstanceId")
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd._get_sass_token(env)
        self.assertIn("Could not find access token", str(cm.exception))


class TestPostRequest(unittest.TestCase):
    """Test the _post_request method"""
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.post')
    def test_post_request_success(self, mock_post):
        """Test successful POST request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}
        mock_post.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        result = wxd._post_request("https://test.com/api", {"key": "value"})
        
        self.assertEqual(result, {"result": "success"})
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs['json'], {"key": "value"})
        self.assertIn("User-Agent", call_kwargs['headers'])
        self.assertFalse(call_kwargs['verify'])
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.post')
    def test_post_request_401_error(self, mock_post):
        """Test POST request with 401 error"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        
        with self.assertRaises(InvalidCredentialsError):
            wxd._post_request("https://test.com/api", {})
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.post')
    def test_post_request_500_error(self, mock_post):
        """Test POST request with 500 error"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Server error"
        mock_post.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        
        with self.assertRaises(TokenRetrievalError):
            wxd._post_request("https://test.com/api", {})
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.post')
    def test_post_request_connection_error(self, mock_post):
        """Test POST request with connection error"""
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd._post_request("https://test.com/api", {})
        self.assertIn("Connection failed", str(cm.exception))


class TestGetHeaders(unittest.TestCase):
    """Test the _get_headers method"""
    
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_headers_success(self, mock_get_env, mock_get_token):
        """Test successful header generation"""
        mock_env = WatsonxDataEnv(CPD, "/auth", "LhInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        wxd = WatsonxData({"instance": "test-instance"}, "host", None)
        headers = wxd._get_headers()
        
        self.assertEqual(headers["Authorization"], "Bearer test_token")
        self.assertEqual(headers["LhInstanceId"], "test-instance")
        self.assertIn("User-Agent", headers)
    
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_headers_no_token(self, mock_get_env, mock_get_token):
        """Test header generation when token retrieval fails"""
        mock_get_env.return_value = Mock()
        mock_get_token.return_value = None
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd._get_headers()
        self.assertIn("Failed to retrieve token for request headers", str(cm.exception))


class TestInternalGetToken(unittest.TestCase):
    """Test the _get_token method (internal)"""
    
    @patch.object(WatsonxData, '_get_cpd_token')
    def test_get_token_cpd(self, mock_cpd):
        """Test _get_token for CPD environment"""
        mock_token = Token("cpd_token")
        mock_cpd.return_value = mock_token
        
        wxd = WatsonxData({"instance": "test"}, "host", None)
        env = WatsonxDataEnv(CPD, "/auth", "Header")
        
        token = wxd._get_token(env)
        
        self.assertEqual(token, mock_token)
        mock_cpd.assert_called_once_with(env)
    
    @patch.object(WatsonxData, '_get_sass_token')
    def test_get_token_saas(self, mock_sass):
        """Test _get_token for SaaS environment"""
        mock_token = Token("sass_token")
        mock_sass.return_value = mock_token
        
        wxd = WatsonxData({"instance": "crn:test"}, "host", None)
        env = WatsonxDataEnv(SAAS, "/auth", "Header")
        
        token = wxd._get_token(env)
        
        self.assertEqual(token, mock_token)
        mock_sass.assert_called_once_with(env)
    
    def test_get_token_unknown_env(self):
        """Test _get_token with unknown environment type"""
        wxd = WatsonxData({"instance": "test"}, "host", None)
        env = WatsonxDataEnv("UNKNOWN", "/auth", "Header")
        
        with self.assertRaises(TokenRetrievalError) as cm:
            wxd._get_token(env)
        self.assertIn("Unknown environment type", str(cm.exception))


class TestGetCatalogDetails(unittest.TestCase):
    """Test the get_catlog_details method"""
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.get')
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_catalog_details_v2_success(self, mock_get_env, mock_get_token, mock_get):
        """Test successful catalog details retrieval for v2"""
        mock_env = WatsonxDataEnv(SAAS, "/auth", "AuthInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "associated_buckets": ["test-bucket"],
            "catalog_type": "iceberg"
        }
        mock_get.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "https://host.com", "/lakehouse/api/v2/catalogs")
        bucket, file_format = wxd.get_catlog_details("test_catalog")
        
        self.assertEqual(bucket, "test-bucket")
        self.assertEqual(file_format, "iceberg")
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.get')
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_catalog_details_v3_success(self, mock_get_env, mock_get_token, mock_get):
        """Test successful catalog details retrieval for v3"""
        mock_env = WatsonxDataEnv(SAAS, "/auth", "AuthInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "associated_storage": ["test-storage"],
            "type": "hive"
        }
        mock_get.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "https://host.com", "/lakehouse/api/v3/catalogs")
        bucket, file_format = wxd.get_catlog_details("test_catalog")
        
        self.assertEqual(bucket, "test-storage")
        self.assertEqual(file_format, "hive")
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.get')
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_catalog_details_404_error(self, mock_get_env, mock_get_token, mock_get):
        """Test catalog details retrieval with 404 error"""
        mock_env = WatsonxDataEnv(SAAS, "/auth", "AuthInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_get.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "https://host.com", None)
        
        with self.assertRaises(CatalogDetailsError) as cm:
            wxd.get_catlog_details("missing_catalog")
        self.assertIn("not found", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.get')
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_catalog_details_401_error(self, mock_get_env, mock_get_token, mock_get):
        """Test catalog details retrieval with 401 error"""
        mock_env = WatsonxDataEnv(SAAS, "/auth", "AuthInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "https://host.com", None)
        
        with self.assertRaises(InvalidCredentialsError):
            wxd.get_catlog_details("test_catalog")
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.get')
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_catalog_details_missing_fields(self, mock_get_env, mock_get_token, mock_get):
        """Test catalog details retrieval with missing fields in response"""
        mock_env = WatsonxDataEnv(SAAS, "/auth", "AuthInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"some_field": "value"}
        mock_get.return_value = mock_response
        
        wxd = WatsonxData({"instance": "test"}, "https://host.com", None)
        
        with self.assertRaises(CatalogDetailsError) as cm:
            wxd.get_catlog_details("test_catalog")
        # The error message will contain details about the missing fields
        self.assertIn("test_catalog", str(cm.exception))
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.requests.get')
    @patch.object(WatsonxData, '_get_token')
    @patch.object(WatsonxData, '_get_environment')
    def test_get_catalog_details_connection_error(self, mock_get_env, mock_get_token, mock_get):
        """Test catalog details retrieval with connection error"""
        mock_env = WatsonxDataEnv(SAAS, "/auth", "AuthInstanceId")
        mock_get_env.return_value = mock_env
        mock_token = Token("test_token")
        mock_get_token.return_value = mock_token
        
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        wxd = WatsonxData({"instance": "test"}, "https://host.com", None)
        
        with self.assertRaises(CatalogDetailsError) as cm:
            wxd.get_catlog_details("test_catalog")
        self.assertIn("Connection failed", str(cm.exception))


if __name__ == '__main__':
    unittest.main()

# Made with Bob