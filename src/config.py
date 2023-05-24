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
    MONGO_CONNECTION_STRING = "mongodb://root:root@172.24.65.106:30001/"
    MONGO_DATABASE = "brokerage"
    OPENAPI_URL = ""
    ORIGINS = "*"
    ROOT_PATH = ""
    SWAGGER_TITLE = "Marketer Admin"
    VERSION = "0.1.0"


settings = Settings()
