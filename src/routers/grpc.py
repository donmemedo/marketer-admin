"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from fastapi.exceptions import RequestValidationError
from src.auth.authentication import get_role_permission
from src.tools.database import get_database
from src.schemas.marketer import *
from src.tools.utils import *
from src.tools.queries import *
from pymongo import MongoClient
from src.auth.authorization import authorize


marketer = APIRouter(prefix="/marketer")
marketer_relation = APIRouter(prefix="/marketer-relation")
grpc = APIRouter(prefix="/grpc")

@marketer.get(
    "/sync",
    tags=["GRPC"],
)
@authorize(
    [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.Update",
        "MarketerAdmin.Marketer.All",
    ]
)
async def sync_marketers(
    request: Request,
    args: Pages = Depends(Pages),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (AddMarketerIn, optional): _description_. Defaults to Depends(AddMarketerIn).

    Returns:
        _type_: _description_
    """
