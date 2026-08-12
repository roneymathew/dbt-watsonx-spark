import unittest
from unittest.mock import Mock, patch, MagicMock
from dbt.adapters.watsonx_spark.http_auth.authenticator import Authenticator, get_authenticator


class ConcreteAuthenticator(Authenticator):
    """Concrete implementation for testing abstract Authenticator class"""
    
    def Authenticate(self, transport):
        """Concrete implementation of abstract method"""
        return transport


class TestAuthenticator(unittest.TestCase):
    """Test the Authenticator base class"""
    
    def test_authenticator_init_with_profile(self):
        """Test Authenticator initialization with profile"""
        profile = {"type": "test_type", "user": "test_user"}
        auth = ConcreteAuthenticator(profile)
        
        self.assertEqual(auth.profile, profile)
        self.assertEqual(auth.type, "test_type")
        self.assertIsNone(auth._token)
        self.assertIsNone(auth._valid_till)
    
    def test_authenticator_init_without_profile(self):
        """Test Authenticator initialization without profile (None)"""
        auth = ConcreteAuthenticator(None)
        
        self.assertEqual(auth.profile, {})
        self.assertIsNone(auth.type)
        self.assertIsNone(auth._token)
        self.assertIsNone(auth._valid_till)
    
    def test_authenticator_init_empty_profile(self):
        """Test Authenticator initialization with empty profile"""
        auth = ConcreteAuthenticator({})
        
        self.assertEqual(auth.profile, {})
        self.assertIsNone(auth.type)
    
    def test_authenticate_abstract_method(self):
        """Test that Authenticate is abstract and must be implemented"""
        # Try to instantiate base class directly - should fail
        with self.assertRaises(TypeError):
            Authenticator({"type": "test"})
    
    def test_concrete_authenticate_implementation(self):
        """Test concrete implementation of Authenticate"""
        auth = ConcreteAuthenticator({"type": "test"})
        mock_transport = Mock()
        
        result = auth.Authenticate(mock_transport)
        self.assertEqual(result, mock_transport)


class TestGetAuthenticator(unittest.TestCase):
    """Test the get_authenticator factory function"""
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.WatsonxData')
    def test_get_authenticator_with_dict_profile(self, mock_wxd):
        """Test get_authenticator with dictionary profile"""
        profile = {"type": "watsonx", "user": "test"}
        host = "test-host.com"
        uri = "/test/uri"
        
        get_authenticator(profile, host, uri)
        
        # Verify WatsonxData was called with correct parameters
        mock_wxd.assert_called_once()
        call_args = mock_wxd.call_args
        
        # Check profile dict was copied and modified
        profile_arg = call_args[0][0]
        self.assertEqual(profile_arg["type"], "watsonx")
        self.assertEqual(profile_arg["user"], "test")
        self.assertTrue(profile_arg["suppress_ssl_warnings"])
        
        # Check host and uri
        self.assertEqual(call_args[0][1], host)
        self.assertEqual(call_args[0][2], uri)
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.WatsonxData')
    def test_get_authenticator_with_none_profile(self, mock_wxd):
        """Test get_authenticator with None profile"""
        get_authenticator(None, "host", "/uri")
        
        mock_wxd.assert_called_once()
        profile_arg = mock_wxd.call_args[0][0]
        
        # Should create empty dict with suppress_ssl_warnings
        self.assertEqual(profile_arg, {"suppress_ssl_warnings": True})
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.WatsonxData')
    def test_get_authenticator_with_string_profile(self, mock_wxd):
        """Test get_authenticator with string profile"""
        get_authenticator("custom_type", "host", "/uri")
        
        mock_wxd.assert_called_once()
        profile_arg = mock_wxd.call_args[0][0]
        
        # Should convert string to dict with type key
        self.assertEqual(profile_arg["type"], "custom_type")
        self.assertTrue(profile_arg["suppress_ssl_warnings"])
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.WatsonxData')
    def test_get_authenticator_with_suppress_ssl_false(self, mock_wxd):
        """Test get_authenticator with suppress_ssl_warnings=False"""
        profile = {"type": "test"}
        
        get_authenticator(profile, "host", "/uri", suppress_ssl_warnings=False)
        
        mock_wxd.assert_called_once()
        profile_arg = mock_wxd.call_args[0][0]
        
        self.assertFalse(profile_arg["suppress_ssl_warnings"])
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.WatsonxData')
    def test_get_authenticator_with_none_host(self, mock_wxd):
        """Test get_authenticator with None host"""
        get_authenticator({"type": "test"}, None, "/uri")
        
        mock_wxd.assert_called_once()
        # Should convert None host to empty string
        self.assertEqual(mock_wxd.call_args[0][1], "")
    
    @patch('dbt.adapters.watsonx_spark.http_auth.wxd_authenticator.WatsonxData')
    def test_get_authenticator_preserves_original_profile(self, mock_wxd):
        """Test that get_authenticator doesn't modify original profile dict"""
        original_profile = {"type": "test", "user": "original"}
        original_copy = original_profile.copy()
        
        get_authenticator(original_profile, "host", "/uri")
        
        # Original profile should be unchanged
        self.assertEqual(original_profile, original_copy)
        self.assertNotIn("suppress_ssl_warnings", original_profile)


if __name__ == '__main__':
    unittest.main()

# Made with Bob
