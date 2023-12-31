"""_summary_

Returns:
    _type_: _description_
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """_summary_

    Args:
        BaseSettings (_type_): _description_
    """

    API_PREFIX: str = ""
    DOCS_URL: str = ""
    MONGO_CONNECTION_STRING: str = (
        "mongodb://root:root@172.24.65.105:30001/?timeoutMS=2000"
    )

    MONGO_DATABASE: str = "brokerage"
    CUSTOMER_COLLECTION: str = "customers"
    MARKETER_COLLECTION: str = "MarketerTable"
    FACTOR_COLLECTION: str = "MarketerFactor"
    FIRMS_COLLECTION: str = "firms"
    TRADES_COLLECTION: str = "trades"
    RELATIONS_COLLECTION: str = "mrelations"

    CONTRACT_COLLECTION: str = "MarketerContract"
    CONTRACT_COEFFICIENT_COLLECTION: str = "MarketerContractCoefficient"
    CONTRACT_DEDUCTION_COLLECTION: str = "MarketerContractDeduction"
    REF_CODE_COLLECTION: str = "MarketerRefCode"

    OPENAPI_URL: str = ""
    ORIGINS: str = "*"
    ROOT_PATH: str = ""
    SWAGGER_TITLE: str = "Marketer Admin"
    VERSION: str = "1.5.4"
    # Added from Marketer
    JWKS_CONFIGURATION_URL: str = (
        "https://cluster.tech1a.co/.well-known/openid-configuration/jwks"
    )
    ISSUER: str = "https://cluster.tech1a.co"
    APPLICATION_ID: str = "d7f48c21-2a19-4bdb-ace8-48928bff0eb5"
    TOKEN_URL: str = "https://cluster.tech1a.co/connect/token"
    CLIENT_ID: str = "M2M.RegisterServicePermission"
    CLIENT_SECRET: str = "IDPRegisterServicePermission"
    GRANT_TYPE: str = "client_credentials"
    OPENID_CONFIGURATION_URL: str = (
        "https://cluster.tech1a.co/.well-known/openid-configuration"
    )
    REGISTRATION_URL: str = (
        "https://cluster.tech1a.co/api/service-permossion/register-service-permission"
    )
    GRPC_IP: str = "172.24.65.20"
    GRPC_PORT: int = 9035
    SPLUNK_HOST: str = "172.24.65.206"
    SPLUNK_PORT: int = 5141
    SPLUNK_INDEX: str = "dev"

    TBS_TRADES_URL: str = "https://tbs.onlinetavana.ir/ClearingSettlement/ClrsReport/StockTradeSummaryAjaxLoadGrid?_dc=1687751713895&action=read"
    TBS_PORTFOLIOS_URL: str = (
        "https://tbs.onlinetavana.ir/CustomerManagement/Customer/AjaxReadPortfolio"
    )
    TBS_CUSTOMERS_URL: str = "https://tbs.onlinetavana.ir/CustomerManagement/Customer/TseAjaxRead?_dc=1692619454394&action=read"
    DATE_STRING: str = "%Y-%m-%d"
    FASTAPI_DOCS: str = "/docs"
    FASTAPI_REDOC: str = "/redoc"


settings = Settings()
