"""_summary_
Returns:
    _type_: _description_
"""
from pymongo import MongoClient

from src.config import settings


def get_database():
    """_summary_

    Returns:
        _type_: _description_
    """
    client = MongoClient(settings.MONGO_CONNECTION_STRING)
    database = client[settings.MONGO_DATABASE]

    return database
