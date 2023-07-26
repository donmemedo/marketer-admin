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
from src.schemas.client_user import *
from src.tools.utils import *
from pymongo import MongoClient
from src.auth.authorization import authorize


client_user = APIRouter(prefix="/client/user")


@client_user.get(
    "/get-marketer",
    # dependencies=[Depends(JWTBearer())],
    tags=["Client - User"],
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





from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pymongo import ASCENDING, MongoClient

from src.auth.authentication import get_current_user
from src.auth.authorization import authorize
# from src.schemas.schemas import ResponseListOut, UserSearchIn
from src.tools.database import get_database
from src.tools.utils import get_marketer_name

# client_user = APIRouter(prefix="/user", tags=["User"])


@client_user.get("/search", response_model=None)
@authorize(["Marketer.All"])
async def get_user_profile(
        user: dict = Depends(get_current_user),
        args: UserSearchIn = Depends(UserSearchIn),
        brokerage: MongoClient = Depends(get_database),
):
    # check whether marketer exists or not and return his name
    query_result = brokerage.marketers.find_one({"IdpId": user.get("sub")})

    if query_result is None:
        return HTTPException(status_code=401, detail="Unauthorized User")

    marketer_fullname = get_marketer_name(query_result)

    pipeline = [
        {"$match": {"$and": [{"Referer": marketer_fullname}]}},
        {
            "$project": {
                "Name": {"$concat": ["$FirstName", " ", "$LastName"]},
                "RegisterDate": 1,
                "Mobile": 1,
                "BankAccountNumber": 1,
                "Username": 1,
                "TradeCode": "$PAMCode",
                "FirstName": 1,
                "LastName": 1,
                "_id": 0,
                "FirmTitle": 1,
                "Telephone": 1,
                "FirmRegisterDate": 1,
                "Email": 1,
                "ActivityField": 1,
            }
        },
        {
            "$match": {
                "$or": [
                    {"Name": {"$regex": args.name}},
                    {"FirmTitle": {"$regex": args.name}},
                ]
            }
        },
        {"$sort": {"RegisterDate": ASCENDING}},
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "items": [
                    {"$skip": (args.page_index - 1) * args.page_size},
                    {"$limit": args.page_size},
                ],
            }
        },
        {"$unwind": "$metadata"},
        {
            "$project": {
                "total": "$metadata.total",
                "items": 1,
            }
        },
    ]

    results = brokerage.customers.aggregate(pipeline=pipeline)

    result_dict = next(results, None)

    if result_dict:
        result = {
            "pagedData": result_dict.get("items", []),
            "errorCode": None,
            "errorMessage": None,
            "totalCount": result_dict.get("total", 0),
        }

        return ResponseListOut(timeGenerated=datetime.now(), result=result, error="")
    else:
        result = {
            "pagedData": [],
            "errorCode": None,
            "errorMessage": None,
            "totalCount": 0,
        }

        return ResponseListOut(timeGenerated=datetime.now(), result=result, error="")
