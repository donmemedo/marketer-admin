"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from fastapi_pagination.ext.pymongo import paginate
from khayyam import JalaliDatetime as jd

from src.schemas.subuser import (
    SubUserIn,
    SubCostIn,
    UsersTotalPureIn,
    UsersListIn,
    TotalUsersListIn,
    MarketerIdpIdIn,
    ResponseListOut,
)
from src.tools.database import get_database
from src.tools.queries import *
from src.tools.utils import peek, to_gregorian_

subuser = APIRouter(prefix="/subuser")


@subuser.get(
    "/list",
    tags=["SubUser"],
    response_model=None,
)
async def search_marketer_user(
    request: Request, args: MarketerIdpIdIn = Depends(MarketerIdpIdIn)
):
    """Gets List of ALL Marketers

    Args:
        request (Request): _description_

    Returns:
        _type_: MarketerOut
    """
    marketer_id = args.idpid
    brokerage = get_database()
    customer_coll = brokerage["customers"]
    firms_coll = brokerage["firms"]
    marketers_coll = brokerage["marketers"]
    query_result = marketers_coll.find({"IdpId": marketer_id})
    marketer_dict = peek(query_result)
    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )
    results = []
    query_result1 = customer_coll.find({"Referer": marketer_fullname}, {"_id": False})
    users = dict(enumerate(query_result1))
    query_result2 = firms_coll.find({"Referer": marketer_fullname}, {"_id": False})
    firms = dict(enumerate(query_result2))
    for i in range(len(users)):
        results.append(users[i])
    for i in range(len(firms)):
        results.append(firms[i])
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@subuser.get(
    "/profile",
    tags=["SubUser"],
    response_model=None,
)
async def get_user_profile(request: Request, args: SubUserIn = Depends(SubUserIn)):
    """Gets List of Users of a Marketer and can search them

    Args:
        request (Request): _description_
        args (UserIn, optional): _description_. Defaults to Depends(UserIn).

    Returns:
        _type_: _description_
    """
    brokerage = get_database()

    customer_coll = brokerage["customers"]
    firms_coll = brokerage["firms"]
    marketers_coll = brokerage["marketers"]
    query_result = marketers_coll.find({"IdpId": marketer_id})

    marketer_dict = peek(query_result)

    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )

    query = {
        "$and": [
            {"Referer": marketer_fullname},
            {"FirstName": {"$regex": args.first_name}},
            {"LastName": {"$regex": args.last_name}},
            {"PAMCode": args.pamcode},
        ]
    }
    query_result = customer_coll.find_one(query, {"_id": False})
    if query_result is None:
        query_result = firms_coll.find_one(query, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@subuser.get(
    "/search",
    tags=["SubUser"],
    response_model=None,
)
async def search_user_profile(request: Request, args: SubUserIn = Depends(SubUserIn)):
    """_summary_

    Args:
        request (Request): _description_
        args (UserIn, optional): _description_. Defaults to Depends(UserIn).

    Returns:
        _type_: _description_
    """
    brokerage = get_database()

    customer_coll = brokerage["customers"]
    marketers_coll = brokerage["marketers"]

    query_result = marketers_coll.find({"IdpId": marketer_id})

    marketer_dict = peek(query_result)

    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )
    if args.username:
        filter = {
            "Referer": {"$regex": marketer_fullname},
            "FirstName": {"$regex": args.first_name},
            "LastName": {"$regex": args.last_name},
            "RegisterDate": {"$regex": args.register_date},
            "Phone": {"$regex": args.phone},
            "Mobile": {"$regex": args.mobile},
            "Username": {"$regex": args.username},
        }
    else:
        filter = {
            "Referer": {"$regex": marketer_fullname},
            "FirstName": {"$regex": args.first_name},
            "LastName": {"$regex": args.last_name},
            "RegisterDate": {"$regex": args.register_date},
            "Phone": {"$regex": args.phone},
            "Mobile": {"$regex": args.mobile},
        }
    return paginate(customer_coll, filter, sort=[("RegisterDate", -1)])


@subuser.get("/cost", tags=["SubUser"], response_model=None)
async def call_subuser_cost(request: Request, args: SubCostIn = Depends(SubCostIn)):
    """_summary_

    Args:
        request (Request): _description_
        args (CostIn, optional): _description_. Defaults to Depends(CostIn).

    Returns:
        _type_: _description_
    """

    brokerage = get_database()
    customers_coll = brokerage["customers"]
    trades_coll = brokerage["trades"]
    marketers_coll = brokerage["marketers"]
    query_result = marketers_coll.find({"IdpId": marketer_id})
    marketer_dict = peek(query_result)
    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )
    query = {
        "Referer": marketer_fullname,
        "FirstName": {"$regex": args.first_name},
        "LastName": {"$regex": args.last_name},
        "Phone": {"$regex": args.phone},
        "Mobile": {"$regex": args.mobile},
        "Username": {"$regex": args.username},
    }
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
        group_by_total_stage("id"),
        project_pure_stage(),
    ]

    subuser_total = next(brokerage.trades.aggregate(pipeline=pipeline), [])
    resp = {
        "result": subuser_total,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {"message": "Null", "code": "Null"},
    }
    return JSONResponse(status_code=200, content=resp)


@subuser.get(
    "/costlist",
    tags=["SubUser"],
    response_model=None,
)
async def marketer_subuser_lists(
    request: Request, args: UsersTotalPureIn = Depends(UsersTotalPureIn)
):
    """_summary_

    Args:
        request (Request): _description_
        args (UsersTotalPureIn, optional): _description_. Defaults to Depends(UsersTotalPureIn).

    Returns:
        _type_: _description_
    """
    database = get_database()
    customers_coll = database["customers"]
    trades_coll = database["trades"]
    marketers_coll = database["marketers"]
    firms_coll = database["firms"]
    query_result = marketers_coll.find({"IdpId": marketer_id})
    marketer_dict = peek(query_result)
    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )
    subusers_query = customers_coll.find(
        {"Referer": marketer_fullname},
        {
            "FirstName": 1,
            "LastName": 1,
            "_id": 0,
            "PAMCode": 1,
            "Mobile": 1,
            "RegisterDate": 1,
            "Phone": 1,
            "Username": 1,
        },
    )
    subusers_list = list(subusers_query)

    results = []
    for subuser in subusers_list:
        response_dict = {}
        query = {"PAMCode": subuser["PAMCode"]}
        customers_records = customers_coll.find(query, {})
        firms_records = firms_coll.find(query, {})
        trade_codes = [c.get("PAMCode") for c in customers_records] + [
            c.get("PAMCode") for c in firms_records
        ]
        today_date = jd.today().date()
        lm_to_gregorian_date = today_date.replace(day=1) + timedelta(days=-1)
        lm_from_gregorian_date = lm_to_gregorian_date.replace(day=1)
        from_gregorian_date = to_gregorian_(args.from_date)
        lmd_from_gregorian_date = to_gregorian_(str(lm_from_gregorian_date))

        if not args.to_date:
            args.to_date = jd.today().date().isoformat()
        to_gregorian_date = to_gregorian_(args.to_date)
        lmd_to_gregorian_date = to_gregorian_(str(lm_to_gregorian_date))

        to_gregorian_date = datetime.strptime(
            to_gregorian_date, "%Y-%m-%d"
        ) + timedelta(days=1)
        lmd_to_gregorian_date = datetime.strptime(
            lmd_to_gregorian_date, "%Y-%m-%d"
        ) + timedelta(days=1)
        last_month = jd.strptime(args.to_date, "%Y-%m-%d").month - 1
        if last_month < 1:
            last_month = last_month + 12
        if last_month < 10:
            last_month = "0" + str(last_month)
        to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")
        lmd_to_gregorian_date = lmd_to_gregorian_date.strftime("%Y-%m-%d")

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
        lm_buy_pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": lmd_from_gregorian_date}},
                        {"TradeDate": {"$lte": lmd_to_gregorian_date}},
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

        lm_sell_pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": lmd_from_gregorian_date}},
                        {"TradeDate": {"$lte": lmd_to_gregorian_date}},
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

        lm_buy_agg_result = peek(trades_coll.aggregate(pipeline=lm_buy_pipeline))
        lm_sell_agg_result = peek(trades_coll.aggregate(pipeline=lm_sell_pipeline))

        lm_buy_dict = {"vol": 0, "fee": 0}

        lm_sell_dict = {"vol": 0, "fee": 0}

        if lm_buy_agg_result:
            lm_buy_dict["vol"] = lm_buy_agg_result.get("TotalBuy")
            lm_buy_dict["fee"] = lm_buy_agg_result.get("TotalFee")

        if lm_sell_agg_result:
            lm_sell_dict["vol"] = lm_sell_agg_result.get("TotalSell")
            lm_sell_dict["fee"] = lm_sell_agg_result.get("TotalFee")

        response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
        response_dict["FirstName"] = subuser.get("FirstName")
        response_dict["LastName"] = subuser.get("LastName")
        response_dict["LMTPV"] = lm_buy_dict.get("vol") + lm_sell_dict.get("vol")
        response_dict["LMTF"] = lm_buy_dict.get("fee") + lm_sell_dict.get("fee")
        response_dict["TradesCount"] = trades_coll.count_documents(
            {"PAMCode": subuser.get("PAMCode")}
        )
        results.append(response_dict)
    if args.sorted:
        results.sort(key=lambda x: x["TotalFee"], reverse=args.asc_desc_TF)
        results.sort(key=lambda x: x["TotalPureVolume"], reverse=args.asc_desc_TPV)
        results.sort(key=lambda x: x["LMTF"], reverse=args.asc_desc_LMTF)
        results.sort(key=lambda x: x["LMTPV"], reverse=args.asc_desc_LMTPV)
        results.sort(key=lambda x: x["FirstName"], reverse=args.asc_desc_FN)
        results.sort(key=lambda x: x["LastName"], reverse=args.asc_desc_LN)
        results.sort(key=lambda x: x["TradesCount"], reverse=args.asc_desc_UC)

    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@subuser.get(
    "/users-total",
    tags=["SubUser"],
    response_model=None,
)
def users_list_by_volume(request: Request, args: UsersListIn = Depends(UsersListIn)):
    """_summary_

    Args:
        request (Request): _description_
        args (UsersListIn, optional): _description_. Defaults to Depends(UsersListIn).

    Returns:
        _type_: _description_
    """
    database = get_database()

    customers_coll = database["customers"]
    trades_coll = database["trades"]
    firms_coll = database["firms"]
    marketers_coll = database["marketers"]
    marketers_query = marketers_coll.find(
        {"IdpId": {"$exists": True, "$not": {"$size": 0}}},
        {"FirstName": 1, "LastName": 1, "_id": 0, "IdpId": 1},
    )
    marketers_list = list(marketers_query)

    results = []
    for marketer in marketers_list:
        if marketer.get("FirstName") == "":
            marketer_fullname = marketer.get("LastName")
        elif marketer.get("LastName") == "":
            marketer_fullname = marketer.get("FirstName")
        else:
            marketer_fullname = (
                marketer.get("FirstName") + " " + marketer.get("LastName")
            )
        query = {"$and": [{"Referer": marketer_fullname}]}
        fields = {"PAMCode": 1}
        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records] + [
            c.get("PAMCode") for c in firms_records
        ]

        from_gregorian_date = args.from_date
        to_gregorian_date = (
            datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

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
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TradeCode": "$_id",
                    "TotalPureVolume": 1,
                    "TotalFee": 1,
                }
            },
            {
                "$lookup": {
                    "from": "firms",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "FirmProfile",
                },
            },
            {"$unwind": {"path": "$FirmProfile", "preserveNullAndEmptyArrays": True}},
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "UserProfile",
                }
            },
            {"$unwind": {"path": "$UserProfile", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "TradeCode": 1,
                    "TotalFee": 1,
                    "TotalPureVolume": 1,
                    "Refferer": "$FirmProfile.Referer",
                    "Referer": "$UserProfile.Referer",
                    "FirmTitle": "$FirmProfile.FirmTitle",
                    "FirmRegisterDate": "$FirmProfile.FirmRegisterDate",
                    "FirmBankAccountNumber": "$FirmProfile.BankAccountNumber",
                    "FirstName": "$UserProfile.FirstName",
                    "LastName": "$UserProfile.LastName",
                    "Username": "$UserProfile.Username",
                    "Mobile": "$UserProfile.Mobile",
                    "RegisterDate": "$UserProfile.RegisterDate",
                    "BankAccountNumber": "$UserProfile.BankAccountNumber",
                }
            },
            {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
            {
                "$facet": {
                    "metadata": [{"$count": "totalCount"}],
                    "items": [
                        {"$skip": (args.page - 1) * args.size},
                        {"$limit": args.size},
                    ],
                }
            },
            {"$unwind": "$metadata"},
            {
                "$project": {
                    "totalCount": "$metadata.totalCount",
                    "items": 1,
                }
            },
        ]

        aggr_result = trades_coll.aggregate(pipeline=pipeline)
        aggre_dict = next(aggr_result, None)
        if aggre_dict is not None:
            results.append(aggre_dict)
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@subuser.get(
    "/total-users",
    tags=["SubUser"],
    response_model=None,
)
def total_users_cost(
    request: Request, args: TotalUsersListIn = Depends(TotalUsersListIn)
):
    """_summary_

    Args:
        request (Request): _description_
        args (TotalUsersListIn, optional): _description_. Defaults to Depends(TotalUsersListIn).

    Returns:
        _type_: _description_
    """
    database = get_database()
    customers_coll = database["customers"]
    trades_coll = database["trades"]
    firms_coll = database["firms"]
    marketers_coll = database["marketers"]
    marketers_query = marketers_coll.find(
        {"IdpId": {"$exists": True, "$not": {"$size": 0}}},
        {"FirstName": 1, "LastName": 1, "_id": 0, "IdpId": 1},
    )
    marketers_list = list(marketers_query)
    results = []
    total_codes = []
    for marketer in marketers_list:
        if marketer.get("FirstName") == "":
            marketer_fullname = marketer.get("LastName")
        elif marketer.get("LastName") == "":
            marketer_fullname = marketer.get("FirstName")
        else:
            marketer_fullname = (
                marketer.get("FirstName") + " " + marketer.get("LastName")
            )
        from_gregorian_date = args.from_date
        to_gregorian_date = (
            datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        query = {"$and": [{"Referer": marketer_fullname}]}
        fields = {"PAMCode": 1}
        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        total_codes = (
            total_codes
            + [c.get("PAMCode") for c in customers_records]
            + [c.get("PAMCode") for c in firms_records]
        )

    for trade_codes in total_codes:
        pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": trade_codes},
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
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TradeCode": "$_id",
                    "TotalPureVolume": 1,
                    "TotalFee": 1,
                }
            },
            {
                "$lookup": {
                    "from": "firms",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "FirmProfile",
                },
            },
            {"$unwind": {"path": "$FirmProfile", "preserveNullAndEmptyArrays": True}},
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "UserProfile",
                }
            },
            {"$unwind": {"path": "$UserProfile", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "TradeCode": 1,
                    "TotalFee": 1,
                    "TotalPureVolume": 1,
                    "Refferer": "$FirmProfile.Referer",
                    "Referer": "$UserProfile.Referer",
                    "FirmTitle": "$FirmProfile.FirmTitle",
                    "FirmRegisterDate": "$FirmProfile.FirmRegisterDate",
                    "FirmBankAccountNumber": "$FirmProfile.BankAccountNumber",
                    "FirstName": "$UserProfile.FirstName",
                    "LastName": "$UserProfile.LastName",
                    "Username": "$UserProfile.Username",
                    "Mobile": "$UserProfile.Mobile",
                    "RegisterDate": "$UserProfile.RegisterDate",
                    "BankAccountNumber": "$UserProfile.BankAccountNumber",
                }
            },
            {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
            {
                "$facet": {
                    "metadata": [{"$count": "totalCount"}],
                    "items": [
                        {"$skip": (args.page - 1) * args.size},
                        {"$limit": args.size},
                    ],
                }
            },
            {"$unwind": "$metadata"},
            {
                "$project": {
                    "items": 1,
                }
            },
        ]
        aggr_result = peek(trades_coll.aggregate(pipeline=pipeline))
        if aggr_result is not None:
            results.append(aggr_result)
    dicter = []
    for i in range(len(results)):
        dicter.append(results[i]["items"][0])

    if args.sorted:
        dicter.sort(key=lambda x: x["TotalFee"], reverse=args.asc_desc_TF)
        dicter.sort(key=lambda x: x["TotalPureVolume"], reverse=args.asc_desc_TPV)

    return ResponseListOut(
        result=dicter, timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"), error=""
    )


add_pagination(subuser)


def cost_calculator(trade_codes, from_date, to_date, page=1, size=10):
    """_summary_

    Args:
        trade_codes (_type_): _description_
        from_date (_type_): _description_
        to_date (_type_): _description_
        page (int, optional): _description_. Defaults to 1.
        size (int, optional): _description_. Defaults to 10.

    Returns:
        _type_: _description_
    """
    database = get_database()
    trades_coll = database["trades"]
    from_gregorian_date = from_date
    to_gregorian_date = (
        datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

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
            }
        },
        {
            "$project": {
                "_id": 0,
                "TradeCode": "$_id",
                "TotalPureVolume": 1,
                "TotalFee": 1,
            }
        },
        {
            "$lookup": {
                "from": "firms",
                "localField": "TradeCode",
                "foreignField": "PAMCode",
                "as": "FirmProfile",
            },
        },
        {"$unwind": {"path": "$FirmProfile", "preserveNullAndEmptyArrays": True}},
        {
            "$lookup": {
                "from": "customers",
                "localField": "TradeCode",
                "foreignField": "PAMCode",
                "as": "UserProfile",
            }
        },
        {"$unwind": {"path": "$UserProfile", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "TradeCode": 1,
                "TotalFee": 1,
                "TotalPureVolume": 1,
                "Refferer": "$FirmProfile.Referer",
                "Referer": "$UserProfile.Referer",
                "FirmTitle": "$FirmProfile.FirmTitle",
                "FirmRegisterDate": "$FirmProfile.FirmRegisterDate",
                "FirmBankAccountNumber": "$FirmProfile.BankAccountNumber",
                "FirstName": "$UserProfile.FirstName",
                "LastName": "$UserProfile.LastName",
                "Username": "$UserProfile.Username",
                "Mobile": "$UserProfile.Mobile",
                "RegisterDate": "$UserProfile.RegisterDate",
                "BankAccountNumber": "$UserProfile.BankAccountNumber",
            }
        },
        {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
        {
            "$facet": {
                "metadata": [{"$count": "totalCount"}],
                "items": [{"$skip": (page - 1) * size}, {"$limit": size}],
            }
        },
        {"$unwind": "$metadata"},
        {
            "$project": {
                "totalCount": "$metadata.totalCount",
                "items": 1,
            }
        },
    ]

    aggr_result = trades_coll.aggregate(pipeline=pipeline)

    aggre_dict = next(aggr_result, None)

    if aggre_dict is None:
        return {}

    aggre_dict["page"] = page
    aggre_dict["size"] = size
    aggre_dict["pages"] = -(aggre_dict.get("totalCount") // -size)
    return aggre_dict
