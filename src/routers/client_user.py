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


from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pymongo import ASCENDING, MongoClient

from src.auth.authentication import get_current_user
from src.auth.authorization import authorize

# from src.schemas.schemas import ResponseListOut, UserSearchIn
from src.tools.database import get_database
from src.tools.utils import get_marketer_name


client_user = APIRouter(prefix="/client/user")


@client_user.get(
    "/search",
    # dependencies=[Depends(JWTBearer())],
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
    # print("Hello World!!!")
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Client.Read",
        "MarketerAdmin.Client.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    # brokerage = get_database()
    marketers_coll = brokerage["marketers"]
    perPage = 10
    page = 1
    # total_count = 1
    # db.collection.find({})
    if args.IdpID:
        query_result = marketers_coll.find_one({"IdpId": args.IdpID}, {"_id": False})
        # query_result = brokerage.marketers.find_one({"IdpId": user.get("sub")})

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
            # {"$match": {"$and": [{"Referer": marketer_fullname}]}},
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
        # result["pagedData"] = results
        # if not args.IdpID:
        result["PageSize"] = args.size
        result["PageNumber"] = args.page
        # result["totalCount"] = total_count

        resp = {
            "result": result,
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "Null",
                "code": "Null",
            },
        }
        return JSONResponse(status_code=200, content=resp)
    # return ResponseListOut(timeGenerated=datetime.now(), result=result, error="")
    else:
        result = {}
        result["pagedData"] = []
        result["errorCode"] = None
        result["errorMessage"] = None
        result["totalCount"] = 0
        result["code"] = "Null"
        result["message"] = "Null"
        # result["pagedData"] = results
        # if not args.IdpID:
        result["PageSize"] = args.size
        result["PageNumber"] = args.page
        # result["totalCount"] = total_count

        resp = {
            "result": result,
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "Null",
                "code": "Null",
            },
        }
        return JSONResponse(status_code=200, content=resp)
