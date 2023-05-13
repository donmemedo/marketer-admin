"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta#, date
from fastapi import APIRouter, Depends, Request
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate
from khayyam import JalaliDatetime as jd
from src.schemas.subuser import (
    SubUserIn,
    SubUserOut,
    MarketerOut,
    # CostIn,
    SubCostIn,
    UsersTotalPureIn,
    UsersListIn,
    TotalUsersListIn,
    MarketerIdpIdIn,
    ResponseOut, ResponseListOut

)
from src.tools.database import get_database
from src.tools.tokens import JWTBearer, get_sub
from src.tools.utils import peek, to_gregorian_

subuser = APIRouter(prefix="/subuser")


@subuser.get(
    "/list", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None#, response_model=Page[SubUserOut]
)
async def search_marketer_user(request: Request, args: MarketerIdpIdIn = Depends(MarketerIdpIdIn)):
    """Gets List of ALL Marketers

    Args:
        request (Request): _description_

    Returns:
        _type_: MarketerOut
    """
    # get user id
    marketer_id = args.idpid
    brokerage = get_database()
    customer_coll = brokerage["customers"]
    firms_coll = brokerage["firms"]
    marketers_coll = brokerage["marketers"]

    # check if marketer exists and return his name
    query_result = marketers_coll.find({"IdpId": marketer_id})
    marketer_dict = peek(query_result)
    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )
    results = []
    query_result1 = customer_coll.find({"Referer": marketer_fullname}, {'_id': False})
    users=dict(enumerate(query_result1))
    query_result2 = firms_coll.find({"Referer": marketer_fullname}, {'_id': False})
    firms=dict(enumerate(query_result2))
    for i in range(len(users)):
        results.append(users[i])
    for i in range(len(firms)):
        results.append(firms[i])
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error=""
        )



@subuser.get(
    "/profile", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None#, response_model=Page[SubUserOut]
)
async def get_user_profile(request: Request, args: SubUserIn = Depends(SubUserIn)):
    """Gets List of Users of a Marketer and can search them

    Args:
        request (Request): _description_
        args (UserIn, optional): _description_. Defaults to Depends(UserIn).

    Returns:
        _type_: _description_
    """
    # get user id
    marketer_id = get_sub(request)
    brokerage = get_database()

    customer_coll = brokerage["customers"]
    firms_coll = brokerage["firms"]
    marketers_coll = brokerage["marketers"]

    # check if marketer exists and return his name
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
            {"PAMCode": args.pamcode}
        ]
    }
    query_result = customer_coll.find_one(query, {'_id': False})
    if query_result is None:
        query_result = firms_coll.find_one(query, {'_id': False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error=""
        )



@subuser.get(
    "/search", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None#, response_model=Page[SubUserOut]
)
async def search_user_profile(request: Request, args: SubUserIn = Depends(SubUserIn)):
    """_summary_

    Args:
        request (Request): _description_
        args (UserIn, optional): _description_. Defaults to Depends(UserIn).

    Returns:
        _type_: _description_
    """
    # get user id
    marketer_id = get_sub(request)
    brokerage = get_database()

    customer_coll = brokerage["customers"]
    marketers_coll = brokerage["marketers"]

    # check if marketer exists and return his name
    query_result = marketers_coll.find({"IdpId": marketer_id})

    marketer_dict = peek(query_result)

    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )

    # query = {"$and": [
    #     {"Referer": marketer_fullname},
    #     {"FirstName": {"$regex": args.first_name}},
    #     {"LastName": {"$regex": args.last_name}}
    #     ]
    # }
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
    # print(query)
    # sort = list({
    #                 'BirthDate': -1
    #             }.items())
    return paginate(customer_coll, filter, sort=[("RegisterDate", -1)])


@subuser.get("/cost", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None)
async def call_subuser_cost(request: Request, args: SubCostIn = Depends(SubCostIn)):
    """_summary_

    Args:
        request (Request): _description_
        args (CostIn, optional): _description_. Defaults to Depends(CostIn).

    Returns:
        _type_: _description_
    """

    # get user id
    marketer_id = get_sub(request)
    brokerage = get_database()
    customers_coll = brokerage["customers"]
    trades_coll = brokerage["trades"]
    marketers_coll = brokerage["marketers"]
    query_result = marketers_coll.find({"IdpId": marketer_id})
    marketer_dict = peek(query_result)
    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )

    # ToDo:Because of having username isn't optional so it will have been changed to IDP or username
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
    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
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
                    "$add": ["$TotalCommission", {"$multiply": ["$Price", "$Volume"]}]
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
    subuser_total = {"TotalPureVolume": 0, "TotalFee": 0}

    if buy_agg_result and sell_agg_result:
        total_buy = buy_agg_result.get("TotalBuy")
        total_sell = sell_agg_result.get("TotalSell")
        subuser_total["TotalFee"] = sell_agg_result.get(
            "TotalFee"
        ) + buy_agg_result.get("TotalFee")
    else:
        total_buy = 0
        total_sell = 0
    subuser_total["TotalPureVolume"] = total_sell + total_buy

    # tpv = subuser_total.get("TotalPureVolume")
    return {
        "TotalBuy": total_buy,
        "TotalSell": total_sell,
        "TotalPureVolume": total_sell + total_buy,
        "TotalFee": subuser_total.get("TotalFee"),
    }


@subuser.get("/costlist", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None)
async def marketer_subuser_lists(
    request: Request, args: UsersTotalPureIn = Depends(UsersTotalPureIn)
):
    # get all current marketers
    database = get_database()
    marketer_id = get_sub(request)
    customers_coll = database["customers"]
    trades_coll = database["trades"]
    marketers_coll = database["marketers"]
    firms_coll = database["firms"]
    # totals_coll = database["totals"]
    query_result = marketers_coll.find({"IdpId": marketer_id})
    marketer_dict = peek(query_result)
    marketer_fullname = (
        marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
    )

    # get all marketers IdpId

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
        # if subuser.get("FirstName") == "":
        #     subuser_fullname = subuser.get("LastName")
        # elif subuser.get("LastName") == "":
        #     subuser_fullname = subuser.get("FirstName")
        # else:
        #     subuser_fullname = subuser.get("FirstName") + " " + subuser.get("LastName")

        # Check if customer exist
        # query = {"Referer": {"$regex": marketer_fullname}}
        query = {"PAMCode": subuser["PAMCode"]}

        # fields = {"PAMCode": 1}

        # customers_records = customers_coll.find(query, fields)
        # firms_records = firms_coll.find(query, fields)
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

        #####################
        if not args.to_date:
            args.to_date = jd.today().date().isoformat()
        #####################

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
            last_month_str = str(jd.strptime(args.to_date, "%Y-%m-%d").year) + str(
                last_month)
        if last_month == 12:
            last_month_str = str(jd.strptime(args.to_date, "%Y-%m-%d").year - 1) + str(
                last_month
            )
        # print(last_month_str)
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
        ######################
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

        ######################
        response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
        response_dict["FirstName"] = subuser.get("FirstName")
        response_dict["LastName"] = subuser.get("LastName")
        ###########
        # lmtpv = last_month_str + "TPV"
        # lmtf = last_month_str + "TF"
        response_dict["LMTPV"] = lm_buy_dict.get("vol") + lm_sell_dict.get("vol")
        response_dict["LMTF"] = lm_buy_dict.get("fee") + lm_sell_dict.get("fee")
        response_dict["TradesCount"] = trades_coll.count_documents(
            {"PAMCode": subuser.get("PAMCode")}
        )
        ###########
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
        error=""
        )

    return results

@subuser.get("/users-total", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None)
def users_list_by_volume(request: Request, args: UsersListIn = Depends(UsersListIn)):
    # get user id
    marketer_id = get_sub(request)
    db = get_database()

    customers_coll = db["customers"]
    trades_coll = db["trades"]
    firms_coll = db["firms"]
    marketers_coll = db["marketers"]

    # check if marketer exists and return his name
    # query_result = marketers_coll.find({"IdpId": marketer_id})

    marketers_query = marketers_coll.find(
        {"IdpId": {"$exists": True, "$not": {"$size": 0}}},
        {"FirstName": 1, "LastName": 1, "_id": 0, "IdpId": 1},
    )
    marketers_list = list(marketers_query)

    # marketer_dict = peek(query_result)

    results = []
    for marketer in marketers_list:
        response_dict = {}
        if marketer.get("FirstName") == "":
            marketer_fullname = marketer.get("LastName")
        elif marketer.get("LastName") == "":
            marketer_fullname = marketer.get("FirstName")
        else:
            marketer_fullname = (
                marketer.get("FirstName") + " " + marketer.get("LastName")
            )
        """
        customers_query = customers_coll.find(
            {"Referer": {"$regex": marketer_fullname}},
            {"FirstName": 1, "LastName": 1, "_id": 0, "PAMCode": 1},
        )
        customers_list = list(customers_query)

        firms_query = firms_coll.find(
            {"Referer": {"$regex": marketer_fullname}},
            {"FirstName": 1, "LastName": 1, "_id": 0, "PAMCode": 1},
        )
        firms_list = list(firms_query)

        for customer in customers_list:
            vv


        for firm in firms_list:
            vv
        # Check if customer exist
        query = {"Referer": {"$regex": marketer_fullname}}

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records] + [
            c.get("PAMCode") for c in firms_records
        ]

        """
        from_gregorian_date = to_gregorian_(args.from_date)
        to_gregorian_date = to_gregorian_(args.to_date)
        to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(days=1)
        to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

        # get all customers' TradeCodes
        query = {"$and": [
            {"Referer": marketer_fullname}
        ]
        }

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        trade_codes = [c.get('PAMCode') for c in customers_records] + [c.get('PAMCode') for c in firms_records]

        pipeline = [
            {
                "$match": {
                    # "TradeCode": {"$in": trade_codes}
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}},
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}}
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
                                    {"$multiply": ["$Price", "$Volume"]}
                                ]
                            },
                            "else": {
                                "$subtract": [
                                    {"$multiply": ["$Price", "$Volume"]},
                                    "$TotalCommission"
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$TradeCode",
                    "TotalFee": {
                        "$sum": "$TradeItemBroker"
                    },
                    "TotalPureVolume": {"$sum": "$Commission"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TradeCode": "$_id",
                    "TotalPureVolume": 1,
                    "TotalFee": 1
                }
            },
            #########Danial's Code########
            # {
            #     "$lookup": {
            #         "from": "customers",
            #         "localField": "TradeCode",
            #         "foreignField": "PAMCode",
            #         "as": "UserProfile"
            #     }
            # },
            # {
            #     "$unwind": "$UserProfile"
            # },
            # {
            #     "$project": {
            #         "TradeCode": 1,
            #         "TotalFee": 1,
            #         "TotalPureVolume": 1,
            #         "FirstName": "$UserProfile.FirstName",
            #         "LastName": "$UserProfile.LastName",
            #         "Username": "$UserProfile.Username",
            #         "Mobile": "$UserProfile.Mobile",
            #         "RegisterDate": "$UserProfile.RegisterDate",
            #         "BankAccountNumber": "$UserProfile.BankAccountNumber",
            #     }
            # },
            # {
            #     "$sort": {
            #         "TotalPureVolume": 1,
            #         "RegisterDate": 1,
            #         "TradeCode": 1
            #     }
            # },
            ##############END of Danial's Code#########
            ##############Refactored Code#########
            {
                "$lookup": {
                    "from": "firms",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "FirmProfile"
                },
            },
            {
                "$unwind": {
                    "path": "$FirmProfile",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "UserProfile"
                }
            },
            {
                "$unwind": {
                    "path": "$UserProfile",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "TradeCode": 1,
                    "TotalFee": 1,
                    "TotalPureVolume": 1,
                    "Refferer": "$FirmProfile.Referer",
                    "Referer": "$UserProfile.Referer",
                    "FirmTitle": "$FirmProfile.FirmTitle",
                    # "FirmRegisterDate": "$FirmTitle.RegisterDate",
                    # "FirmBankAccountNumber": "$FirmTitle.BankAccountNumber",
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

            {
                "$sort": {
                    "TotalPureVolume": 1,
                    "RegisterDate": 1,
                    "TradeCode": 1
                }
            },
            ###########END of Refactor############
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "items": [
                        {"$skip": (args.page - 1) * args.size},
                        {"$limit": args.size}
                    ]
                }
            },
            {
                "$unwind": "$metadata"
            },
            {
                "$project": {
                    "total": "$metadata.total",
                    "items": 1,
                }
            }
        ]

        aggr_result = trades_coll.aggregate(pipeline=pipeline)
        aggre_dict = next(aggr_result, None)
        if aggre_dict is not None:
            results.append(aggre_dict)
        # results.append(aggre_dict)
    # aggre_dict = next(aggr_result, None)

    # if aggre_dict is None:
    #     return {}

    # aggre_dict["page"] = 1#args.page
    # aggre_dict["size"] = 1000000#args.size
    # aggre_dict["pages"] = - (aggre_dict.get("total") // - args.size)

    # return aggre_dict

    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error=""
        )


    return results


@subuser.get("/total-users", dependencies=[Depends(JWTBearer())],tags=["SubUser"], response_model=None)
def total_users_cost(request: Request, args: TotalUsersListIn = Depends(TotalUsersListIn)):
    # get user id
    marketer_id = get_sub(request)
    db = get_database()

    customers_coll = db["customers"]
    trades_coll = db["trades"]
    firms_coll = db["firms"]
    marketers_coll = db["marketers"]

    # check if marketer exists and return his name
    # query_result = marketers_coll.find({"IdpId": marketer_id})

    marketers_query = marketers_coll.find(
        {"IdpId": {"$exists": True, "$not": {"$size": 0}}},
        {"FirstName": 1, "LastName": 1, "_id": 0, "IdpId": 1},
    )
    marketers_list = list(marketers_query)

    # marketer_dict = peek(query_result)

    results = []
    total_codes = []
    for marketer in marketers_list:
        response_dict = {}
        if marketer.get("FirstName") == "":
            marketer_fullname = marketer.get("LastName")
        elif marketer.get("LastName") == "":
            marketer_fullname = marketer.get("FirstName")
        else:
            marketer_fullname = (
                marketer.get("FirstName") + " " + marketer.get("LastName")
            )
        """
        customers_query = customers_coll.find(
            {"Referer": {"$regex": marketer_fullname}},
            {"FirstName": 1, "LastName": 1, "_id": 0, "PAMCode": 1},
        )
        customers_list = list(customers_query)

        firms_query = firms_coll.find(
            {"Referer": {"$regex": marketer_fullname}},
            {"FirstName": 1, "LastName": 1, "_id": 0, "PAMCode": 1},
        )
        firms_list = list(firms_query)

        for customer in customers_list:
            vv


        for firm in firms_list:
            vv
        # Check if customer exist
        query = {"Referer": {"$regex": marketer_fullname}}

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records] + [
            c.get("PAMCode") for c in firms_records
        ]

        """
        from_gregorian_date = to_gregorian_(args.from_date)
        to_gregorian_date = to_gregorian_(args.to_date)
        to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(days=1)
        to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

        # get all customers' TradeCodes
        query = {"$and": [
            {"Referer": marketer_fullname}
        ]
        }

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        total_codes = total_codes + [c.get('PAMCode') for c in customers_records] + [c.get('PAMCode') for c in firms_records]


    for trade_codes in total_codes:


        pipeline = [
            {
                "$match": {
                    # "TradeCode": {"$in": trade_codes}
                    "$and": [
                        # {"TradeCode": {"$in": trade_codes}},
                        {"TradeCode": trade_codes},
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}}
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
                                    {"$multiply": ["$Price", "$Volume"]}
                                ]
                            },
                            "else": {
                                "$subtract": [
                                    {"$multiply": ["$Price", "$Volume"]},
                                    "$TotalCommission"
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$TradeCode",
                    "TotalFee": {
                        "$sum": "$TradeItemBroker"
                    },
                    "TotalPureVolume": {"$sum": "$Commission"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TradeCode": "$_id",
                    "TotalPureVolume": 1,
                    "TotalFee": 1
                }
            },
            #########Danial's Code########
            # {
            #     "$lookup": {
            #         "from": "customers",
            #         "localField": "TradeCode",
            #         "foreignField": "PAMCode",
            #         "as": "UserProfile"
            #     }
            # },
            # {
            #     "$unwind": "$UserProfile"
            # },
            # {
            #     "$project": {
            #         "TradeCode": 1,
            #         "TotalFee": 1,
            #         "TotalPureVolume": 1,
            #         "FirstName": "$UserProfile.FirstName",
            #         "LastName": "$UserProfile.LastName",
            #         "Username": "$UserProfile.Username",
            #         "Mobile": "$UserProfile.Mobile",
            #         "RegisterDate": "$UserProfile.RegisterDate",
            #         "BankAccountNumber": "$UserProfile.BankAccountNumber",
            #     }
            # },
            # {
            #     "$sort": {
            #         "TotalPureVolume": 1,
            #         "RegisterDate": 1,
            #         "TradeCode": 1
            #     }
            # },
            ##############END of Danial's Code#########
            ##############Refactored Code#########
            {
                "$lookup": {
                    "from": "firms",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "FirmProfile"
                },
            },
            {
                "$unwind": {
                    "path": "$FirmProfile",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "TradeCode",
                    "foreignField": "PAMCode",
                    "as": "UserProfile"
                }
            },
            {
                "$unwind": {
                    "path": "$UserProfile",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "TradeCode": 1,
                    "TotalFee": 1,
                    "TotalPureVolume": 1,
                    "Refferer": "$FirmProfile.Referer",
                    "Referer": "$UserProfile.Referer",
                    "FirmTitle": "$FirmProfile.FirmTitle",
                    # "FirmRegisterDate": "$FirmTitle.RegisterDate",
                    # "FirmBankAccountNumber": "$FirmTitle.BankAccountNumber",
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

            {
                "$sort": {
                    "TotalPureVolume": 1,
                    "RegisterDate": 1,
                    "TradeCode": 1
                }
            },
            ###########END of Refactor############
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "items": [
                        {"$skip": (args.page - 1) * args.size},
                        {"$limit": args.size}
                    ]
                }
            },
            {
                "$unwind": "$metadata"
            },
            {
                "$project": {
                    # "total": "$metadata.total",
                    "items": 1,
                }
            }
        ]


        # aggr_result = trades_coll.aggregate(pipeline=pipeline)
        aggr_result = peek(trades_coll.aggregate(pipeline=pipeline))
        # aggre_dict = next(aggr_result, None)
        # if aggre_dict is not None:
        #     results.append(aggre_dict)
        if aggr_result is not None:
            results.append(aggr_result)
        # results.append(aggre_dict)
    # aggre_dict = next(aggr_result, None)

    # if aggre_dict is None:
    #     return {}
    # aggre_dict["page"] = 1#args.page
    # aggre_dict["size"] = 1000000#args.size
    # aggre_dict["pages"] = - (aggre_dict.get("total") // - args.size)

    # return aggre_dict
    dicter =[]
    # dicter['itemss']= {}
    for i in range(len(results)):
        # dicter['itemss'][i]=results[i]['items'][0]
        dicter.append(results[i]['items'][0])

    if args.sorted:
    # if 1==1:
        dicter.sort(key=lambda x: x["TotalFee"], reverse=args.asc_desc_TF)
        dicter.sort(key=lambda x: x["TotalPureVolume"], reverse=args.asc_desc_TPV)




    return ResponseListOut(
        result=dicter,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error=""
        )


    return dicter


add_pagination(subuser)


def buy_pipe(customers_records, from_gregorian_date, to_gregorian_date):
    trade_codes = [c.get("PAMCode") for c in customers_records]
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
                    "$add": ["$TotalCommission", {"$multiply": ["$Price", "$Volume"]}]
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
    return buy_pipeline


def sell_pipe(customers_records, from_gregorian_date, to_gregorian_date):
    trade_codes = [c.get("PAMCode") for c in customers_records]
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
    return sell_pipeline
