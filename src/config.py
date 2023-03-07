from pydantic import BaseSettings


class Settings(BaseSettings):
    API_PREFIX = ""
    DOCS_URL = ""
    MONGO_CONNECTION_STRING = "mongodb://root:1qaz1qaz@localhost:27017/"
    MONGO_DATABASE = "brokerage"
    OPENAPI_URL = ""
    ORIGINS = "*"
    ROOT_PATH = ""
    SWAGGER_TITLE = "Marketer Admin"
    VERSION = "0.0.1"


settings = Settings()