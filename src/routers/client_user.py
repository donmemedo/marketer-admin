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
from src.schemas.client_user import *
from src.tools.utils import *
from pymongo import MongoClient
from fastapi import APIRouter, Depends, HTTPException
from pymongo import ASCENDING, MongoClient
from src.auth.authorization import authorize
from src.tools.database import get_database
from src.tools.utils import get_marketer_name


client_user = APIRouter(prefix="/client/user")


@client_user.get(
    "/search",
    tags=["Client - User"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Client.Read",
        "MarketerAdmin.Client.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
)
async def get_user_profile(
    request: Request,
    args: GetUserSearchIn = Depends(GetUserSearchIn),
    brokerage: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    marketers_coll = brokerage["marketers"]
    if args.IdpID:
        query_result = marketers_coll.find_one({"IdpId": args.IdpID}, {"_id": False})
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
    else:
        pipeline = [
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
        result = {}
        result["pagedData"] = result_dict.get("items", [])
        result["errorCode"] = None
        result["errorMessage"] = None
        result["totalCount"] = result_dict.get("total", 0)
        result["code"] = "Null"
        result["message"] = "Null"
        result["PageSize"] = args.size
        result["PageNumber"] = args.page
        resp = {
            "result": result,
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "Null",
                "code": "Null",
            },
        }
        return JSONResponse(status_code=200, content=resp)
    else:
        result = {}
        result["pagedData"] = []
        result["errorCode"] = None
        result["errorMessage"] = None
        result["totalCount"] = 0
        result["code"] = "Null"
        result["message"] = "Null"
        result["PageSize"] = args.size
        result["PageNumber"] = args.page
        resp = {
            "result": result,
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "Null",
                "code": "Null",
            },
        }
        return JSONResponse(status_code=200, content=resp)
