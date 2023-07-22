"""_summary_

Returns:
    _type_: _description_
"""
from pydantic import BaseSettings


class Settings(BaseSettings):
    """_summary_

    Args:
        BaseSettings (_type_): _description_
    """

    API_PREFIX = ""
    DOCS_URL = ""
    MONGO_CONNECTION_STRING = "mongodb://root:root@172.24.65.105:30001/"

    MONGO_DATABASE = "brokerage"
    CUSTOMER_COLLECTION = "customers"
    FIRMS_COLLECTION = "firms"
    TRADES_COLLECTION = "trades"

    OPENAPI_URL = ""
    ORIGINS = "*"
    ROOT_PATH = ""
    SWAGGER_TITLE = "Marketer Admin"
    VERSION = "0.1.1"
    ROOT_PATH = ""
    # Added from Marketer
    JWKS_CONFIGURATION_URL = (
        "https://cluster.tech1a.co/.well-known/openid-configuration/jwks"
    )
    ISSUER = "https://cluster.tech1a.co"
    APPLICATION_ID = "d7f48c21-2a19-4bdb-ace8-48928bff0eb5"
    TOKEN_URL = "https://cluster.tech1a.co/connect/token"
    CLIENT_ID = "M2M.RegisterServicePermission"
    CLIENT_SECRET = "IDPRegisterServicePermission"
    GRANT_TYPE = "client_credentials"
    OPENID_CONFIGURATION_URL = (
        "https://cluster.tech1a.co/.well-known/openid-configuration"
    )
    REGISTRATION_URL = (
        "https://cluster.tech1a.co/api/service-permossion/register-service-permission"
    )

    SPLUNK_HOST = "172.24.65.206"
    SPLUNK_PORT = 5141
    SPLUNK_INDEX = "dev"


settings = Settings()
