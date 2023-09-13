"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.client_volume_and_fee import *
from src.tools.database import get_database
from src.tools.queries import *
from src.tools.utils import get_marketer_name
from src.config import settings

client_volume_and_fee = APIRouter(prefix="/client/volume-and-fee")


@client_volume_and_fee.get(
    "/user-total",
    tags=["Client - Volume and Fee"],
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
async def get_user_total_trades(
    request: Request,
    args: GetUserTotalIn = Depends(GetUserTotalIn),
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
    marketers_coll = brokerage[settings.MARKETER_COLLECTION]
    customers_coll = brokerage[settings.CUSTOMER_COLLECTION]
    trades_coll = brokerage[settings.TRADES_COLLECTION]
    factors_coll = brokerage[settings.FACTOR_COLLECTION]
    from_gregorian_date = args.from_date
    to_gregorian_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    pipeline = [
        filter_users_stage([args.trade_code], from_gregorian_date, to_gregorian_date),
        project_commission_stage(),
        group_by_total_stage("$TradeCode"),
        project_pure_stage(),
        join_customers_stage(),
        unwind_user_stage(),
        project_fields_stage(),
    ]
    res = next(brokerage.trades.aggregate(pipeline=pipeline), None)
    resp = {
        "result": res,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(status_code=200, content=resp)


@client_volume_and_fee.get(
    "/marketer-total",
    tags=["Client - Volume and Fee"],
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
async def get_marketer_total_trades(
    request: Request,
    args: GetMarketerTotalIn = Depends(GetMarketerTotalIn),
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
    marketers_coll = brokerage[settings.MARKETER_COLLECTION]
    customers_coll = brokerage[settings.CUSTOMER_COLLECTION]
    trades_coll = brokerage[settings.TRADES_COLLECTION]
    factors_coll = brokerage[settings.FACTOR_COLLECTION]

    marketers_query = (
        marketers_coll.find(
            # {"MarketerID": {"$exists": True, "$not": {"$size": 0}}},
            # {"FirstName": 1, "LastName": 1, "_id": 0, "MarketerID": 1},
            {"TbsReagentId": {"$exists": True, "$not": {"$size": 0}}},
            {"TbsReagentName": 1, "ReagentRefLink": 1, "_id": 0, "MarketerID": 1},
        )
        .skip(args.size * (args.page - 1))
        .limit(args.size)
    )
    if args.IdpID:
        # marketers_query = marketers_coll.find({"MarketerID": args.IdpID}, {"_id": False})
        marketers_query = marketers_coll.find({"MarketerID": args.IdpID}, {"_id": False})

    marketers_list = list(marketers_query)
    total_count = marketers_coll.count_documents(
        # {"MarketerID": {"$exists": True, "$not": {"$size": 0}}}
        {"TbsReagentId": {"$exists": True, "$not": {"$size": 0}}}
    )

    results = []
    for marketer in marketers_list:
        marketer_total = {}
        # marketer_fullname = get_marketer_name(marketer)
        # query = {"Referer": {"$regex": marketer_fullname}}
        query = {"Referer": marketer["TbsReagentName"]}
        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)

        trade_codes = [c.get("PAMCode") for c in customers_records]

        from_gregorian_date = args.from_date
        to_gregorian_date = (
            datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        pipeline = [
            filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
            project_commission_stage(),
            group_by_total_stage("MarketerID"),
            project_pure_stage(),
        ]

        marketer_total = next(brokerage.trades.aggregate(pipeline=pipeline), {})
        # marketer_total["FirstName"] = marketer.get("FirstName")
        # marketer_total["LastName"] = marketer.get("LastName")
        marketer_total["TbsReagentName"] = marketer.get("TbsReagentName")
        marketer_total["UsersCount"] = customers_coll.count_documents(
            # {"Referer": {"$regex": marketer_fullname}}
            {"Referer": marketer["TbsReagentName"]}
        )
        results.append(marketer_total)

    result = {}
    result["code"] = "Null"
    result["message"] = "Null"
    result["pagedData"] = results
    if not args.IdpID:
        result["PageSize"] = args.size
        result["PageNumber"] = args.page
        result["totalCount"] = total_count

    resp = {
        "result": result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(status_code=200, content=resp)


@client_volume_and_fee.get(
    "/users-total",
    tags=["Client - Volume and Fee"],
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
async def users_list_by_volume(
    request: Request,
    args: GetUsersListIn = Depends(GetUsersListIn),
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
    marketers_coll = brokerage[settings.MARKETER_COLLECTION]
    customers_coll = brokerage[settings.CUSTOMER_COLLECTION]
    trades_coll = brokerage[settings.TRADES_COLLECTION]
    factors_coll = brokerage[settings.FACTOR_COLLECTION]

    # query_result = marketers_coll.find_one({"MarketerID": args.IdpID}, {"_id": False})
    query_result = marketers_coll.find_one({"MarketerID": args.IdpID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 404})

    # marketer_fullname = get_marketer_name(query_result)
    marketer_fullname = query_result["TbsReagentName"]
    from_gregorian_date = args.from_date
    to_gregorian_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    # query = {"RefererTitle": {"$regex": marketer_fullname}}
    query = {"Referer": marketer_fullname}
    # trade_codes = customers_coll.distinct("TradeCodes", query)
    trade_codes = customers_coll.distinct("PAMCode", query)

    if args.user_type.value == "active":
        pipeline = [
            filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
            project_commission_stage(),
            group_by_total_stage("$TradeCode"),
            project_pure_stage(),
            join_customers_stage(),
            unwind_user_stage(),
            project_fields_stage(),
            sort_stage(args.sort_by.value, args.sort_order.value),
            paginate_data(args.page, args.size),
            unwind_metadata_stage(),
            project_total_stage(),
        ]

        active_dict = next(trades_coll.aggregate(pipeline=pipeline), {})
        result = {}
        result["pagedData"] = active_dict.get("items", [])
        result["errorCode"] = None
        result["errorMessage"] = None
        result["totalCount"] = active_dict.get("total", 0)
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

    elif args.user_type.value == "inactive":
        active_users_pipeline = [
            filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
            group_by_trade_code_stage(),
            project_by_trade_code_stage(),
        ]

        active_users_res = trades_coll.aggregate(pipeline=active_users_pipeline)
        active_users_set = set(i.get("TradeCode") for i in active_users_res)

        # check whether it is empty or not
        inactive_users_set = set(trade_codes) - active_users_set

        inactive_users_pipeline = [
            match_inactive_users(list(inactive_users_set)),
            project_inactive_users(),
            sort_stage(args.sort_by.value, args.sort_order.value),
            paginate_data(args.page, args.size),
            unwind_metadata_stage(),
            project_total_stage(),
        ]

        inactive_dict = next(
            customers_coll.aggregate(pipeline=inactive_users_pipeline), {}
        )

        result = {}
        result["pagedData"] = inactive_dict.get("items", [])
        result["errorCode"] = None
        result["errorMessage"] = None
        result["totalCount"] = inactive_dict.get("total", 0)
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
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "Null",
                "code": "Null",
            },
        }
        return JSONResponse(status_code=200, content=resp)
