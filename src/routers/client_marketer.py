"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd

# from src.tools.tokens import JWTBearer#, get_role_permission
from src.auth.authentication import get_role_permission
from src.tools.database import get_database
from src.schemas.client_marketer import *
from src.tools.utils import *
from pymongo import MongoClient
from src.auth.authorization import authorize


client_marketer = APIRouter(prefix="/client/marketer")


@client_marketer.get(
    "/get-marketer",
    # dependencies=[Depends(JWTBearer())],
    tags=["Client - Marketer"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
)
async def get_marketer_profile(
    request: Request,
    args: ModifyMarketerIn = Depends(ModifyMarketerIn),
    brokerage: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_

    Returns:
        _type_: _description_
    """
    print("Hello World!!!")