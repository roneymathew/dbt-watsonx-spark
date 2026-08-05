import json
import logging
import platform
import re
import warnings
from platform import python_version
from typing import Any, Dict, Optional, Tuple

import requests
from thrift.transport import THttpClient
from urllib3.exceptions import InsecureRequestWarning

from dbt.adapters.watsonx_spark import __version__
from dbt.adapters.watsonx_spark.http_auth.authenticator import Authenticator
from dbt.adapters.watsonx_spark.http_auth.exceptions import (
    CatalogDetailsError,
    InvalidCredentialsError,
    TokenRetrievalError,
)
from dbt.adapters.watsonx_spark.http_auth.status_codes import StatusCodeHandler


CPD = "CPD"
SAAS = "SASS"
SAAS_AZURE = "SAAS_AZURE"
DEFAULT_SASS_URI_VERSION = "v2"
CPD_AUTH_ENDPOINT = "/icp4d-api/v1/authorize"
CPD_AUTH_HEADER = "LhInstanceId"
SASS_AUTH_HEADER = "AuthInstanceId"
DBT_WATSONX_SPARK_VERSION = __version__.version
OS = platform.system()
PYTHON_VERSION = python_version()
USER_AGENT = (
    f"dbt-watsonx-spark/{DBT_WATSONX_SPARK_VERSION} (IBM watsonx.data; Python {PYTHON_VERSION}; {OS})"
)

logger = logging.getLogger(__name__)


class WatsonxDataEnv:
    def __init__(self, envType: str, authEndpoint: str, authInstanceHeaderKey: str):
        self.envType = envType
        self.authEndpoint = authEndpoint
        self.authInstanceHeaderKey = authInstanceHeaderKey


class Token:
    def __init__(self, token: str):
        self.token = token


class WatsonxData(Authenticator):
    VERSION_REGEX = re.compile(r"/api/(v[0-9]+(?:\.[0-9]+)*)\b(?:/|$)")

    def __init__(self, profile: Dict[str, Any], host: str, uri: Optional[str]):
        self.profile = profile
        self.type = profile.get("type")
        self.instance = profile.get("instance")
        self.user = profile.get("user")
        self.apikey = profile.get("apikey")
        self.host = host          # may be redirected to CP host for Azure BYOC auth
        self._lakehouse_host = host  # always the original lakehouse host
        self.uri = uri
        
        # Conditionally suppress SSL warnings based on profile setting
        suppress_ssl_warnings = profile.get("suppress_ssl_warnings", True)
        if suppress_ssl_warnings:
            warnings.filterwarnings('ignore', category=InsecureRequestWarning)
        else:
            # Reset the filter to show warnings
            warnings.filterwarnings('default', category=InsecureRequestWarning)
        
        if self.uri:
            version_from_uri = self._extract_version_from_uri(self.uri)
        else:
            version_from_uri = None

        self.lakehouse_version = (
            version_from_uri
            or DEFAULT_SASS_URI_VERSION
        )
        self.sass_auth_endpoint = f"/lakehouse/api/{self.lakehouse_version}/auth/authenticate"
        # Azure BYOC: auth is done against the Control Plane (CP) URL, not the lakehouse host.
        # The CP URL is discovered via GET <host>/lakehouse/api/<ver>/<instance_guid>/cp_endpoint.
        self._cp_url: Optional[str] = None
        self._instance_guid: Optional[str] = self._extract_instance_guid_from_crn(
            profile.get("instance", "")
        )

        # For Azure BYOC, resolve and cache the CP URL once at init time so every subsequent
        # call to get_token() / _get_headers() / get_catlog_details() reuses the cached value
        # instead of issuing a discovery HTTP request on each invocation.
        if self._is_azure_byoc():
            self.host = self._get_cp_url()

    def _extract_version_from_uri(self, uri: str) -> Optional[str]:
        """
        Extracts version url like 'v3' or 'v3.1' from paths containing '/api/<version>/'.
        Returns None if not found.
        """
        m = self.VERSION_REGEX.search(uri)
        return m.group(1) if m else None

    def _extract_instance_guid_from_crn(self, crn: str) -> Optional[str]:
        """
        Extracts the instance GUID from a CRN string.
        CRN format: crn:version:cname:ctype:service:region:scope:service-instance:resource-type:resource
        The service-instance segment (index 7) holds the GUID, e.g.:
          crn:v1:azure-staging:public:lakehouse:eastus:sub/...:20260709-1328-1753-820b-d86de880ece4::
        Returns None if not found or CRN is not well-formed.
        """
        if not crn:
            return None
        parts = crn.split(":")
        # segment 7 is the service-instance GUID
        if len(parts) > 7 and parts[7]:
            return parts[7]
        return None

    def _is_azure_byoc(self) -> bool:
        """
        Returns True when the CRN identifies an Azure BYOC (Bring Your Own Cloud) deployment.
        Azure BYOC CRNs contain 'azure' as the cloud provider segment (index 2), e.g.:
          crn:v1:azure-staging:public:lakehouse:eastus:...
          crn:v1:azure:public:lakehouse:eastus:...
        """
        if not self.instance:
            return False
        parts = self.instance.split(":")
        if len(parts) >= 3:
            return parts[2].lower().startswith("azure")
        return False

    def _get_cp_url(self) -> str:
        """
        Fetches the Control Plane (CP) hostname for Azure BYOC from the lakehouse host.
        Calls GET <host>/lakehouse/api/<ver>/cp_endpoint which returns a plain-text hostname,
        e.g. "console-azure-canadacentral.lakehouse.dev.saas.ibm.com"
        Result is cached for the lifetime of this authenticator instance.
        """
        if self._cp_url:
            return self._cp_url

        url = f"{self._lakehouse_host}/lakehouse/api/{self.lakehouse_version}/cp_endpoint"
        logger.debug(f"Fetching Azure BYOC control plane URL from: {url}")
        try:
            verify_ssl = not self.profile.get("suppress_ssl_warnings", True)
            response = requests.get(url, headers={"User-Agent": USER_AGENT}, verify=verify_ssl)
            response.raise_for_status()
            # Response is plain-text hostname, e.g. "console-azure-canadacentral.lakehouse.dev.saas.ibm.com"
            hostname = response.text.strip().strip('"').rstrip("%")
            if not hostname:
                raise TokenRetrievalError(
                    message=f"cp_endpoint returned an empty response"
                )
            # Ensure it has an https:// scheme
            if not hostname.startswith("http"):
                hostname = "https://" + hostname
            self._cp_url = hostname.rstrip("/")
            logger.debug(f"Azure BYOC control plane URL: {self._cp_url}")
            return self._cp_url
        except requests.exceptions.RequestException as err:
            raise TokenRetrievalError(
                message=f"Failed to fetch Azure BYOC cp_endpoint: {err}"
            )

    def _get_environment(self) -> WatsonxDataEnv:
        if self.instance and "crn" in self.instance:
            if self._is_azure_byoc():
                # Azure BYOC: self.host was already set to the CP host in __init__.
                # _lakehouse_host retains the original host for catalog/Thrift calls.
                auth_endpoint = (
                    f"/lakehouse/api/{self.lakehouse_version}"
                    f"/{self._instance_guid}/auth/authenticate"
                )
                return WatsonxDataEnv(SAAS_AZURE, auth_endpoint, SASS_AUTH_HEADER)
            return WatsonxDataEnv(SAAS, self.sass_auth_endpoint, SASS_AUTH_HEADER)
        else:
            return WatsonxDataEnv(CPD, CPD_AUTH_ENDPOINT, CPD_AUTH_HEADER)

    def Authenticate(self, transport: THttpClient.THttpClient) -> THttpClient.THttpClient:
        transport.setCustomHeaders(self._get_headers())
        return transport

    def get_token(self) -> str:
        wxd_env = self._get_environment()
        token_obj = self._get_token(wxd_env)
        
        if not token_obj or not hasattr(token_obj, 'token'):
            error_msg = "Failed to retrieve authentication token"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        return str(token_obj.token)

    def _get_cpd_token(self, cpd_env: WatsonxDataEnv) -> Token:
        cpd_url = f"{self.host}{cpd_env.authEndpoint}"
        response = self._post_request(
            cpd_url, data={"username": self.user, "api_key": self.apikey})
            
        if not response or "token" not in response:
            error_msg = "Invalid response format when retrieving CPD token"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        token = Token(response.get("token"))
        return token

    def _get_sass_token(self, sass_env: WatsonxDataEnv) -> Token:
        sass_url = f"{self.host}{sass_env.authEndpoint}"
        response = self._post_request(
            sass_url,
            data={
                "username": "ibmlhapikey_" + self.user if self.user is not None else "ibmlhapikey",
                "password": self.apikey,
                "instance_name": "",
                "instance_id": self.instance,
            })
            
        if not response:
            error_msg = "Invalid response when retrieving SaaS token"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
        
        text = json.dumps(response)
        token_match = re.search(r'"access(?:_)?token"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
        
        if not token_match:
            error_msg = "Could not find access token in response"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        token = Token(token_match.group(1))
        return token

    def _post_request(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            header = {"User-Agent": USER_AGENT}
            response = requests.post(url, json=data, headers=header, verify=False)
            
            # Get the environment type for documentation links
            env_type = self._get_environment().envType if hasattr(self, "_get_environment") else None
            
            # Handle 401 errors specifically with environment-specific documentation links
            if response.status_code == 401:
                success, error = StatusCodeHandler.handle_401_error(
                    response, context="Token retrieval", env_type=env_type
                )
                raise error
            
            # Use the StatusCodeHandler to handle other responses
            success, error_msg = StatusCodeHandler.handle_response(
                response,
                context="Token retrieval",
                error_handlers={
                    **{
                        code: lambda r, msg: (
                            False,
                            TokenRetrievalError(status_code=r.status_code, message=msg),
                        )
                        for code in range(400, 600)
                        if code != 401
                    }
                },
                log_errors=True,
            )
            
            if not success:
                if isinstance(error_msg, Exception):
                    raise error_msg
                raise TokenRetrievalError(message=error_msg)
                
            return response.json()
        except requests.exceptions.RequestException as err:
            error_msg = f"Connection error when retrieving token: {err}"
            logger.error(error_msg)
            raise TokenRetrievalError(message=str(err))
        except (TokenRetrievalError, InvalidCredentialsError):
            # Re-raise these exceptions
            raise
        except Exception as err:
            error_msg = f"Unexpected error when retrieving token: {err}"
            logger.error(error_msg)
            raise TokenRetrievalError(message=str(err))

    def _get_headers(self) -> Dict[str, str]:
        wxd_env = self._get_environment()
        token_obj = self._get_token(wxd_env)
        
        if not token_obj or not hasattr(token_obj, 'token'):
            error_msg = "Failed to retrieve token for request headers"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        auth_header = {"Authorization": f"Bearer {token_obj.token}"}
        instance_header = {
            str(wxd_env.authInstanceHeaderKey): str(self.instance)}
        user_agent = {"User-Agent": USER_AGENT}
        headers = {**auth_header, **instance_header, **user_agent}
        return headers

    def _get_token(self, wxd_env: WatsonxDataEnv) -> Token:
        try:
            if wxd_env.envType == CPD:
                return self._get_cpd_token(wxd_env)
            elif wxd_env.envType in (SAAS, SAAS_AZURE):
                # Both SaaS and Azure BYOC use the same token format.
                # For BYOC, self.host is redirected to the CP host inside _get_environment().
                return self._get_sass_token(wxd_env)
            else:
                error_msg = f"Unknown environment type: {wxd_env.envType}"
                logger.error(error_msg)
                raise TokenRetrievalError(message=error_msg)
        except Exception as e:
            # If the exception is already one of our custom exceptions, re-raise it
            if isinstance(e, (TokenRetrievalError, InvalidCredentialsError)):
                raise
            # Otherwise, wrap it in a TokenRetrievalError
            error_msg = f"Error retrieving token: {str(e)}"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg) from e

    def get_catlog_details(self, catalog_name: str) -> Tuple[str, str]:
        wxd_env = self._get_environment()
        # For Azure BYOC the catalog API is on the CP host (same as auth).
        # For all others use the original lakehouse host.
        api_host = self.host if wxd_env.envType == SAAS_AZURE else self._lakehouse_host
        url = f"{api_host}/lakehouse/api/{self.lakehouse_version}/catalogs/{catalog_name}"
        
        try:
            result = self._get_token(wxd_env)
            if not result or not hasattr(result, 'token'):
                error_msg = "Failed to retrieve token for catalog details request"
                logger.error(error_msg)
                raise TokenRetrievalError(message=error_msg)
                
            header = {
                'Authorization': f"Bearer {result.token}",
                'accept': 'application/json',
                wxd_env.authInstanceHeaderKey: self.instance,
                "User-Agent": USER_AGENT
            }
            
            response = requests.get(url=url, headers=header, verify=False)
            
            env_type = wxd_env.envType
            
            if response.status_code == 401:
                success, error = StatusCodeHandler.handle_401_error(
                    response,
                    context=f"Catalog details retrieval for '{catalog_name}'",
                    env_type=env_type
                )
                raise error
            
            error_handlers = {
                404: lambda r, msg: (False, CatalogDetailsError(
                    catalog_name=catalog_name,
                    status_code=r.status_code,
                    message=f"Catalog '{catalog_name}' not found. Please check the catalog name."
                )),
                # For all other error codes
                **{code: lambda r, msg: (False, CatalogDetailsError(
                    catalog_name=catalog_name,
                    status_code=r.status_code,
                    message=msg
                )) for code in range(400, 600) if code not in [401, 404]}
            }
            
            # Use the StatusCodeHandler to handle the response
            success, error_msg = StatusCodeHandler.handle_response(
                response,
                context=f"Catalog details retrieval for '{catalog_name}'",
                error_handlers=error_handlers,
                log_errors=True
            )
            
            if not success:
                if isinstance(error_msg, Exception):
                    raise error_msg
                raise CatalogDetailsError(catalog_name=catalog_name, message=error_msg)
                
            response_json = response.json()
            
            try:
                if self.lakehouse_version == "v2":
                    bucket = response_json.get("associated_buckets")[0]
                    file_format = response_json.get("catalog_type")
                else:
                    bucket = response_json.get("associated_storage")[0]
                    file_format = response_json.get("type")
                    
                if not bucket or not file_format:
                    error_msg = f"Invalid catalog response format. Missing required fields."
                    logger.error(error_msg)
                    raise CatalogDetailsError(catalog_name=catalog_name, message=error_msg)
                    
                return bucket, file_format
            except (IndexError, KeyError) as err:
                error_msg = f"Invalid catalog response format: {err}"
                logger.error(error_msg)
                raise CatalogDetailsError(catalog_name=catalog_name, message=error_msg)
                
        except requests.exceptions.RequestException as err:
            error_msg = f"Connection error when retrieving catalog details: {err}"
            logger.error(error_msg)
            raise CatalogDetailsError(catalog_name=catalog_name, message=str(err))
        except (TokenRetrievalError, InvalidCredentialsError, CatalogDetailsError):
            # Re-raise these exceptions
            raise
        except Exception as err:
            error_msg = f"Unexpected error when retrieving catalog details: {err}"
            logger.error(error_msg)
            raise CatalogDetailsError(catalog_name=catalog_name, message=str(err))
