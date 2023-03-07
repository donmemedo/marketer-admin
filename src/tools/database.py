from pymongo import MongoClient
from config import settings


def get_database():
    client = MongoClient(settings.MONGO_CONNECTION_STRING)
    database = client[settings.MONGO_DATABASE]

    return database

