"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.client_marketer import *
from src.tools.database import get_database
from src.tools.utils import *
from src.tools.utils import peek, to_gregorian_
from src.config import settings

client_marketer = APIRouter(prefix="/client/marketer")


@client_marketer.get(
    "/get-marketer",
    tags=["Client - Marketer"],
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
async def get_marketer_profile(
    request: Request,
    args: GetMarketerList = Depends(GetMarketerList),
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
    # results = marketers_coll.find_one({"MarketerID": args.IdpID}, {"_id": False})
    results = marketers_coll.find_one({"MarketerID": args.IdpID}, {"_id": False})
    if not args.IdpID:
        total_count = marketers_coll.count_documents({})
        query_result = (
            marketers_coll.find({}, {"_id": 0})
            .skip(args.size * (args.page - 1))
            .limit(args.size)
        )
        results = []
        # marketers = dict(enumerate(query_result))
        # for i in range(len(marketers)):
        #     results.append(marketer_entity(marketers[i]))
        for marketer in query_result:
            results.append(marketer)

    if not results:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 200})
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


@client_marketer.get(
    "/cost",
    tags=["Client - Marketer"],
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
async def cal_marketer_cost(
    request: Request,
    args: GetCostIn = Depends(GetCostIn),
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
    marketers_query = (
        # marketers_coll.find(
        #     {"MarketerID": {"$exists": True, "$not": {"$size": 0}}},
        #     {"FirstName": 1, "LastName": 1, "_id": 0, "MarketerID": 1},
        # )
        marketers_coll.find(
            {"TbsReagentId": {"$exists": True, "$not": {"$size": 0}}},
            {"TbsReagentName": 1, "ReagentRefLink": 1, "_id": 0, "MarketerID": 1},
        )
        .skip(args.size * (args.page - 1))
        .limit(args.size)
    )
    if args.IdpID:
        # marketers_query = marketers_coll.find({"MarketerID": args.IdpID}, {"_id": False})
        marketers_query = marketers_coll.find(
            {"MarketerID": args.IdpID}, {"_id": False}
        )

    marketers_list = list(marketers_query)
    total_count = marketers_coll.count_documents(
        # {"MarketerID": {"$exists": True, "$not": {"$size": 0}}}
        {"TbsReagentId": {"$exists": True, "$not": {"$size": 0}}}
    )
    results = []
    for marketer in marketers_list:
        marketer_total = {}
        # marketer_fullname = get_marketer_name(marketer)
        # query = {"Referer": marketer_fullname}
        query = {"Referer": marketer["TbsReagentName"]}
        fields = {"PAMCode": 1}
        customers_records = customers_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records]
        from_gregorian_date = args.from_date
        to_gregorian_date = (
            datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

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
        # marketer_total["FirstName"] = marketer.get("FirstName")
        # marketer_total["LastName"] = marketer.get("LastName")
        marketer_total["TbsReagentName"] = marketer.get("TbsReagentName")

        marketer_total["UsersCount"] = customers_coll.count_documents(
            # {"Referer": marketer_fullname}
            {"Referer": marketer["TbsReagentName"]}
        )
        marketer_total["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        marketer_total["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
        marketer_plans = {
            "payeh": {"start": 0, "end": 30000000000, "marketer_share": 0.05},
            "morvarid": {
                "start": 30000000000,
                "end": 50000000000,
                "marketer_share": 0.1,
            },
            "firouzeh": {
                "start": 50000000000,
                "end": 100000000000,
                "marketer_share": 0.15,
            },
            "aghigh": {
                "start": 100000000000,
                "end": 150000000000,
                "marketer_share": 0.2,
            },
            "yaghout": {
                "start": 150000000000,
                "end": 200000000000,
                "marketer_share": 0.25,
            },
            "zomorod": {
                "start": 200000000000,
                "end": 300000000000,
                "marketer_share": 0.3,
            },
            "tala": {
                "start": 300000000000,
                "end": 400000000000,
                "marketer_share": 0.35,
            },
            "almas": {"start": 400000000000, "marketer_share": 0.4},
        }
        pure_fee = marketer_total.get("TotalFee") * 0.65
        marketer_fee = 0
        tpv = marketer_total.get("TotalPureVolume")

        if marketer_plans["payeh"]["start"] <= tpv < marketer_plans["payeh"]["end"]:
            marketer_fee = pure_fee * marketer_plans["payeh"]["marketer_share"]
            plan = "Payeh"
            next_plan = marketer_plans["payeh"]["end"] - pure_fee
        elif (
            marketer_plans["morvarid"]["start"]
            <= tpv
            < marketer_plans["morvarid"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["morvarid"]["marketer_share"]
            plan = "Morvarid"
            next_plan = marketer_plans["morvarid"]["end"] - pure_fee
        elif (
            marketer_plans["firouzeh"]["start"]
            <= tpv
            < marketer_plans["firouzeh"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["firouzeh"]["marketer_share"]
            plan = "Firouzeh"
            next_plan = marketer_plans["firouzeh"]["end"] - pure_fee
        elif marketer_plans["aghigh"]["start"] <= tpv < marketer_plans["aghigh"]["end"]:
            marketer_fee = pure_fee * marketer_plans["aghigh"]["marketer_share"]
            plan = "Aghigh"
            next_plan = marketer_plans["aghigh"]["end"] - pure_fee
        elif (
            marketer_plans["yaghout"]["start"] <= tpv < marketer_plans["yaghout"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["yaghout"]["marketer_share"]
            plan = "Yaghout"
            next_plan = marketer_plans["yaghout"]["end"] - pure_fee
        elif (
            marketer_plans["zomorod"]["start"] <= tpv < marketer_plans["zomorod"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["zomorod"]["marketer_share"]
            plan = "Zomorod"
            next_plan = marketer_plans["zomorod"]["end"] - pure_fee
        elif marketer_plans["tala"]["start"] <= tpv < marketer_plans["tala"]["end"]:
            marketer_fee = pure_fee * marketer_plans["tala"]["marketer_share"]
            plan = "Tala"
            next_plan = marketer_plans["tala"]["end"] - pure_fee
        elif marketer_plans["almas"]["start"] <= tpv:
            marketer_fee = pure_fee * marketer_plans["almas"]["marketer_share"]
            plan = "Almas"
            next_plan = 0
        final_fee = marketer_fee
        if args.salary != 0:
            salary = args.salary * marketer_fee
            final_fee -= salary
            if args.insurance != 0:
                insurance = args.insurance * marketer_fee
                final_fee -= insurance

        if args.tax != 0:
            tax = marketer_fee * args.tax
            final_fee -= tax

        if args.collateral != 0:
            collateral = marketer_fee * args.collateral
            final_fee -= collateral
        if args.tax == 0 and args.collateral == 0:
            collateral = marketer_fee * 0.05
            tax = marketer_fee * 0.1
            final_fee -= final_fee * 0.15

        marketer_total["PureFee"] = pure_fee
        marketer_total["MarketerFee"] = marketer_fee
        marketer_total["Plan"] = plan
        marketer_total["Next Plan"] = next_plan
        marketer_total["Tax"] = tax
        marketer_total["Collateral"] = collateral
        marketer_total["FinalFee"] = final_fee
        marketer_total["Payment"] = final_fee

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


@client_marketer.get(
    "/factor-print",
    tags=["Client - Marketer"],
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
async def factor_print(
    request: Request,
    args: GetFactorIn = Depends(GetFactorIn),
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
        marketers_query = marketers_coll.find(
            {"MarketerID": args.IdpID}, {"_id": False}
        )

    marketers_list = list(marketers_query)
    total_count = marketers_coll.count_documents(
        # {"MarketerID": {"$exists": True, "$not": {"$size": 0}}}
        {"TbsReagentId": {"$exists": True, "$not": {"$size": 0}}}
    )
    results = []
    for marketer in marketers_list:
        marketer_total = {}
        try:
            dd = args.year + f"{int(args.month):02}"
            cc = args.year + f"{int(args.month) - 2:02}"
            if args.month == "1" or args.month == "01":
                cc = str(int(args.year) - 1) + "11"
            if args.month == "2" or args.month == "02":
                cc = str(int(args.year) - 1) + "12"
            two_months_ago_coll = marketer[cc + "Collateral"]
            from_date = f"{args.year}-{args.month}-01"
            from_gregorian_date = to_gregorian_(from_date)
            to_date = jd.strptime(from_date, "%Y-%m-%d")
            dorehh = f"{args.year} {to_date.monthname()}"
            to_date = to_date.replace(day=to_date.daysinmonth).strftime("%Y-%m-%d")
            to_gregorian_date = to_gregorian_(to_date)
            to_gregorian_date = datetime.strptime(
                to_gregorian_date, "%Y-%m-%d"
            ) + timedelta(days=1)
            to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

            res = {
                "TotalFee": marketer[dd + "TF"],
                "TotalPureVolume": marketer[dd + "TPV"],
                "PureFee": marketer[dd + "PureFee"],
                "MarketerFee": marketer[dd + "MarFee"],
                "Tax": marketer[dd + "Tax"],
                "Collateral": marketer[dd + "Collateral"],
                "CollateralOfTwoMonthAgo": two_months_ago_coll,
                "Payment": marketer[dd + "Payment"],
            }
            results.append(res)
        except:
            pass
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
