"""_summary_

Returns:
    _type_: _description_
"""
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd

from src.auth.authentication import get_role_permission
from src.schemas.client_volume_and_fee import *
from src.tools.utils import *
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from pymongo import MongoClient
from src.auth.authorization import authorize
from src.tools.database import get_database
from src.tools.utils import get_marketer_name, to_gregorian_


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
    marketers_coll = brokerage["marketers"]
    customers_coll = brokerage["customers"]
    trades_coll = brokerage["trades"]
    factors_coll = brokerage["factors"]
    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "$and": [
                    {"TradeCode": args.trade_code},
                    {"TradeDate": {"$gte": from_gregorian_date}},
                    {"TradeDate": {"$lte": to_gregorian_date}},
                ]
            }
        },
        {
            "$project": {
                "Price": 1,
                "Volume": 1,
                "Total": {"$multiply": ["$Price", "$Volume"]},
                "TotalCommission": 1,
                "PriorityAcceptance": 1,
                "TradeItemBroker": 1,
                "TradeCode": 1,
                "Commission": {
                    "$cond": {
                        "if": {"$eq": ["$TradeType", 1]},
                        "then": {
                            "$add": [
                                "$TotalCommission",
                                {"$multiply": ["$Price", "$Volume"]},
                            ]
                        },
                        "else": {
                            "$subtract": [
                                {"$multiply": ["$Price", "$Volume"]},
                                "$TotalCommission",
                            ]
                        },
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "$TradeCode",
                "TotalFee": {"$sum": "$TradeItemBroker"},
                "TotalPureVolume": {"$sum": "$Commission"},
                "TotalPriorityAcceptance": {"$sum": "$PriorityAcceptance"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "TradeCode": "$_id",
                "TotalPureVolume": {
                    "$add": ["$TotalPriorityAcceptance", "$TotalPureVolume"]
                },
                "TotalFee": 1,
            }
        },
        {
            "$lookup": {
                "from": "customers",
                "localField": "TradeCode",
                "foreignField": "PAMCode",
                "as": "UserProfile",
            }
        },
        {"$unwind": "$UserProfile"},
        {
            "$project": {
                "TradeCode": 1,
                "TotalFee": 1,
                "TotalPureVolume": 1,
                "FirstName": "$UserProfile.FirstName",
                "LastName": "$UserProfile.LastName",
                "Username": "$UserProfile.Username",
                "Mobile": "$UserProfile.Mobile",
                "RegisterDate": "$UserProfile.RegisterDate",
                "BankAccountNumber": "$UserProfile.BankAccountNumber",
                "FirmTitle": "$UserProfile.FirmTitle",
                "Telephone": "$UserProfile.Telephone",
                "FirmRegisterLocation": "$UserProfile.FirmRegisterLocation",
                "Email": "$UserProfile.Email",
                "ActivityField": "$UserProfile.ActivityField",
            }
        },
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
    marketers_coll = brokerage["marketers"]
    customers_coll = brokerage["customers"]
    trades_coll = brokerage["trades"]
    factors_coll = brokerage["factors"]

    marketers_query = (
        marketers_coll.find(
            {"IdpId": {"$exists": True, "$not": {"$size": 0}}},
            {"FirstName": 1, "LastName": 1, "_id": 0, "IdpId": 1},
        )
        .skip(args.size * args.page)
        .limit(args.size)
    )
    if args.IdpID:
        marketers_query = marketers_coll.find({"IdpId": args.IdpID}, {"_id": False})

    marketers_list = list(marketers_query)
    total_count = marketers_coll.count_documents({"IdpId": {"$exists": True, "$not": {"$size": 0}}})

    results = []
    for marketer in marketers_list:
        marketer_total = {}
        marketer_fullname = get_marketer_name(marketer)
        query = {"Referer": {"$regex": marketer_fullname}}

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)

        trade_codes = [
            c.get("PAMCode") for c in customers_records
        ]  # + [c.get("PAMCode") for c in firms_records]

        from_gregorian_date = to_gregorian_(args.from_date)
        to_gregorian_date = to_gregorian_(args.to_date)
        to_gregorian_date = datetime.strptime(
            to_gregorian_date, "%Y-%m-%d"
        ) + timedelta(days=1)
        to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")
        buy_pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}},
                        {"TradeType": 1},
                    ]
                }
            },
            {
                "$project": {
                    "Price": 1,
                    "Volume": 1,
                    "Total": {"$multiply": ["$Price", "$Volume"]},
                    "TotalCommission": 1,
                    "TradeItemBroker": 1,
                    "Buy": {
                        "$add": [
                            "$TotalCommission",
                            {"$multiply": ["$Price", "$Volume"]},
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$id",
                    "TotalFee": {"$sum": "$TradeItemBroker"},
                    "TotalBuy": {"$sum": "$Buy"},
                }
            },
            {"$project": {"_id": 0, "TotalBuy": 1, "TotalFee": 1}},
        ]

        sell_pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}},
                        {"TradeType": 2},
                    ]
                }
            },
            {
                "$project": {
                    "Price": 1,
                    "Volume": 1,
                    "Total": {"$multiply": ["$Price", "$Volume"]},
                    "TotalCommission": 1,
                    "TradeItemBroker": 1,
                    "Sell": {
                        "$subtract": [
                            {"$multiply": ["$Price", "$Volume"]},
                            "$TotalCommission",
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$id",
                    "TotalFee": {"$sum": "$TradeItemBroker"},
                    "TotalSell": {"$sum": "$Sell"},
                }
            },
            {"$project": {"_id": 0, "TotalSell": 1, "TotalFee": 1}},
        ]

        buy_agg_result = peek(trades_coll.aggregate(pipeline=buy_pipeline))
        sell_agg_result = peek(trades_coll.aggregate(pipeline=sell_pipeline))

        buy_dict = {"vol": 0, "fee": 0}

        sell_dict = {"vol": 0, "fee": 0}

        if buy_agg_result:
            buy_dict["vol"] = buy_agg_result.get("TotalBuy")
            buy_dict["fee"] = buy_agg_result.get("TotalFee")

        if sell_agg_result:
            sell_dict["vol"] = sell_agg_result.get("TotalSell")
            sell_dict["fee"] = sell_agg_result.get("TotalFee")

        marketer_total["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        marketer_total["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
        marketer_total["FirstName"] = marketer.get("FirstName")
        marketer_total["LastName"] = marketer.get("LastName")

        marketer_total["UsersCount"] = customers_coll.count_documents(
            {"Referer": {"$regex": marketer_fullname}}
        )
        marketer_total["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        marketer_total["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")

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
    marketers_coll = brokerage["marketers"]
    customers_coll = brokerage["customers"]
    trades_coll = brokerage["trades"]
    factors_coll = brokerage["factors"]

    query_result = marketers_coll.find_one({"IdpId": args.IdpID}, {"_id": False})
    if not query_result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found.")

    marketer_fullname = get_marketer_name(query_result)

    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    query = {"Referer": {"$regex": marketer_fullname}}

    trade_codes = brokerage.customers.distinct("PAMCode", query)

    if args.user_type.value == "active":
        pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}},
                    ]
                }
            },
            {
                "$project": {
                    "Price": 1,
                    "Volume": 1,
                    "Total": {"$multiply": ["$Price", "$Volume"]},
                    "PriorityAcceptance": 1,
                    "TotalCommission": 1,
                    "TradeItemBroker": 1,
                    "TradeCode": 1,
                    "Commission": {
                        "$cond": {
                            "if": {"$eq": ["$TradeType", 1]},
                            "then": {
                                "$add": [
                                    "$TotalCommission",
                                    {"$multiply": ["$Price", "$Volume"]},
                                ]
                            },
                            "else": {
                                "$subtract": [
                                    {"$multiply": ["$Price", "$Volume"]},
                                    "$TotalCommission",
                                ]
                            },
                        }
                    },
                }
            },
            {
                "$group": {
                    "_id": "$TradeCode",
                    "TotalFee": {"$sum": "$TradeItemBroker"},
                    "TotalPureVolume": {"$sum": "$Commission"},
                    "TotalPriorityAcceptance": {"$sum": "$PriorityAcceptance"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TradeCode": "$_id",
                    "TotalPureVolume": {
                        "$add": ["$TotalPriorityAcceptance", "$TotalPureVolume"]
                    },
                    "TotalFee": 1,
                }
            },
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "UserProfile",
                }
            },
            {"$unwind": "$UserProfile"},
            {
                "$project": {
                    "TradeCode": 1,
                    "TotalFee": 1,
                    "TotalPureVolume": 1,
                    "FirstName": "$UserProfile.FirstName",
                    "LastName": "$UserProfile.LastName",
                    "Username": "$UserProfile.Username",
                    "Mobile": "$UserProfile.Mobile",
                    "RegisterDate": "$UserProfile.RegisterDate",
                    "BankAccountNumber": "$UserProfile.BankAccountNumber",
                    "FirmTitle": "$UserProfile.FirmTitle",
                    "Telephone": "$UserProfile.Telephone",
                    "FirmRegisterLocation": "$UserProfile.FirmRegisterLocation",
                    "Email": "$UserProfile.Email",
                    "ActivityField": "$UserProfile.ActivityField",
                }
            },
            {"$sort": {args.sort_by.value: args.sort_order.value}},
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "items": [
                        {"$skip": (args.page - 1) * args.size},
                        {"$limit": args.size},
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

        active_dict = next(brokerage.trades.aggregate(pipeline=pipeline), {})
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
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}},
                    ]
                }
            },
            {"$group": {"_id": "$TradeCode"}},
            {"$project": {"_id": 0, "TradeCode": "$_id"}},
        ]

        active_users_res = brokerage.trades.aggregate(pipeline=active_users_pipeline)
        active_users_set = set(i.get("TradeCode") for i in active_users_res)

        # check wether it is empty or not
        inactive_uesrs_set = set(trade_codes) - active_users_set

        inactive_users_pipline = [
            {"$match": {"PAMCode": {"$in": list(inactive_uesrs_set)}}},
            {
                "$project": {
                    "_id": 0,
                    "TradeCode": 1,
                    "FirstName": 1,
                    "LastName": 1,
                    "Username": 1,
                    "Mobile": 1,
                    "RegisterDate": 1,
                    "BankAccountNumber": 1,
                    "FirmTitle": 1,
                    "Telephone": 1,
                    "FirmRegisterDate": 1,
                    "Email": 1,
                    "ActivityField": 1,
                }
            },
            {"$sort": {args.sort_by.value: args.sort_order.value}},
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "items": [
                        {"$skip": (args.page - 1) * args.size},
                        {"$limit": args.size},
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

        inactive_dict = next(
            brokerage.customers.aggregate(pipeline=inactive_users_pipline), {}
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
