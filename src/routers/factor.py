"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate
from khayyam import JalaliDatetime as jd
from src.tools.tokens import JWTBearer, get_sub
from src.tools.database import get_database
from src.schemas.factor import (
    MarketerOut,
    ModifyMarketerIn,
    UsersTotalPureIn,
    MarketersProfileIn,
    MarketerIn,
    ConstOut,
    ModifyConstIn,
    ResponseOut,
    ResponseListOut,
    ModifyFactorIn,
    FactorsListIn,
)
from src.tools.utils import peek, to_gregorian_, marketer_entity, get_marketer_name


factor = APIRouter(prefix="/factor")


@factor.get(
    "/get-factor-consts",
    dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
    # response_model=Page[ConstOut],
    response_model=None,
)
async def get_factors_consts(request: Request, args: MarketerIn = Depends(MarketerIn)):
    """_summary_

    Args:
        request (Request): _description_

    Returns:
        _type_: _description_
    """
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    marketer_id = args.IdpID
    brokerage = get_database()
    consts_coll = brokerage["consts"]
    query_result = consts_coll.find_one({"MarketerID": marketer_id}, {"_id": False})
    if not query_result:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error={
                "errorMessage":"موردی یافت نشد.",
                "errorCode":"30001"
            },
        )
    else:
        return ResponseListOut(
            result=query_result,
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error="",
        )


@factor.get(
    "/get-all-factor-consts",
    dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
    # response_model=Page[ConstOut],
    response_model=None,
)
async def get_all_factors_consts(request: Request):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    results = []
    consts_coll = database["consts"]
    query_result = consts_coll.find({}, {"_id": False})
    consts = dict(enumerate(query_result))
    for i in range(len(consts)):
        results.append((consts[i]))
    if not results:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error={
                "errorMessage":"موردی برای ثابتهای فاکتورها یافت نشد.",
                "errorCode":"30002"
            },
        )
    else:
        return ResponseListOut(
            result=results,
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error="",
        )


@factor.put(
    "/modify-factor-consts", dependencies=[Depends(JWTBearer())], tags=["Factor"]
)
async def modify_factor_consts(
    request: Request, args: ModifyConstIn = Depends(ModifyConstIn)
):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    consts_coll = database["consts"]
    if args.MarketerID is None:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error={
                "errorMessage":"IDP مارکتر را وارد کنید.",
                "errorCode":"30003"
            },
        )

    filter = {"MarketerID": args.MarketerID}
    update = {"$set": {}}

    if args.FixIncome is not None:
        update["$set"]["FixIncome"] = args.FixIncome

    if args.Insurance is not None:
        update["$set"]["Insurance"] = args.Insurance

    if args.Collateral is not None:
        update["$set"]["Collateral"] = args.Collateral

    if args.Tax is not None:
        update["$set"]["Tax"] = args.Tax

    consts_coll.update_one(filter, update)
    query_result = consts_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})
    # marketer_dict = peek(query_result)
    return ResponseListOut(
        result=query_result,  # marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.put("/modify-factor", dependencies=[Depends(JWTBearer())], tags=["Factor"])
async def modify_factor(
    request: Request, args: ModifyFactorIn = Depends(ModifyFactorIn)
):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    factor_coll = database["factors"]
    if args.MarketerID is None:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error={
                "errorMessage": "IDP مارکتر را وارد کنید.",
                "errorCode": "30003"
            },
        )

    filter = {"IdpID": args.MarketerID}
    update = {"$set": {}}
    per = args.Period

    if args.TotalPureVolume is not None:
        update["$set"][per + "TPV"] = args.TotalPureVolume

    if args.TotalFee is not None:
        update["$set"][per + "TF"] = args.TotalFee

    if args.PureFee is not None:
        update["$set"][per + "PureFee"] = args.PureFee

    if args.MarketerFee is not None:
        update["$set"][per + "MarFee"] = args.MarketerFee

    if args.Plan is not None:
        update["$set"][per + "Plan"] = args.Plan

    if args.Tax is not None:
        update["$set"][per + "Tax"] = args.Tax

    if args.Collateral is not None:
        update["$set"][per + "Collateral"] = args.Collateral

    if args.FinalFee is not None:
        update["$set"][per + "FinalFee"] = args.FinalFee

    if args.Payment is not None:
        update["$set"][per + "Payment"] = args.Payment

    if args.FactorStatus is not None:
        update["$set"][per + "FactStatus"] = args.FactorStatus

    factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False})
    # marketer_dict = peek(query_result)
    return ResponseListOut(
        result=query_result,  # marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.get(
    "/total-factors",
    dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
    response_model=None,
)
def total_factors(request: Request, args: FactorsListIn = Depends(FactorsListIn)):
    # get user id
    marketer_id = get_sub(request)
    db = get_database()

    customers_coll = db["customers"]
    trades_coll = db["trades"]
    firms_coll = db["firms"]
    marketers_coll = db["marketers"]
    factors_coll = db["factors"]

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
        marketer_fullname = get_marketer_name(marketer)
        response_dict = {}
        for year in range(1400, jd.today().year):
            for i in range(12):
                period = f"{year+1}{(i+1):02}"
                try:
                    zizo = factors_coll.find_one({"FullName": marketer_fullname})
                    response_dict[marketer_fullname][
                        period + "FactStatus"
                    ] = factors_coll.find_one({"FullName": marketer_fullname}).get(
                        period + "FactStatus"
                    )
                    response_dict[marketer_fullname][period + "FactStatus"] = zizo.get(
                        period + "FactStatus"
                    )
                except:
                    print(str(year) + "yoyo")
        # from_gregorian_date = to_gregorian_(args.from_date)
        # to_gregorian_date = to_gregorian_(args.to_date)
        # to_gregorian_date = datetime.strptime(
        #     to_gregorian_date, "%Y-%m-%d"
        # ) + timedelta(days=1)
        # to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")
        #
        # # get all customers' TradeCodes
        # query = {"$and": [{"Referer": marketer_fullname}]}
        #
        # fields = {"PAMCode": 1}
        #
        # customers_records = customers_coll.find(query, fields)
        # firms_records = firms_coll.find(query, fields)
        # trade_codes = [c.get("PAMCode") for c in customers_records] + [
        #     c.get("PAMCode") for c in firms_records
        # ]
        querrrry = {
            "$group": {
                "_id": "$IdpID",
            },
        }

        pipeline = [
            {
                "$match": {
                    # "TradeCode": {"$in": trade_codes}
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
            {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
            ###########END of Refactor############
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
        error="",
    )


add_pagination(factor)


def cost_calculator(trade_codes, from_date, to_date, page=1, size=10):
    db = get_database()
    trades_coll = db["trades"]
    from_gregorian_date = to_gregorian_(from_date)
    to_gregorian_date = to_gregorian_(to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

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

    # return ResponseOut(
    #     result=aggre_dict,
    #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
    #     error=""
    #     )


def totaliter(marketer_fullname, from_gregorian_date, to_gregorian_date):
    database = get_database()

    customers_coll = database["customers"]
    trades_coll = database["trades"]
    # marketers_coll = database["marketers"]
    firms_coll = database["firms"]

    query = {"Referer": {"$regex": marketer_fullname}}

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    firms_records = firms_coll.find(query, fields)
    trade_codes = [c.get("PAMCode") for c in customers_records] + [
        c.get("PAMCode") for c in firms_records
    ]

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

    buy_dict = {"vol": 0, "fee": 0}

    sell_dict = {"vol": 0, "fee": 0}

    if buy_agg_result:
        buy_dict["vol"] = buy_agg_result.get("TotalBuy")
        buy_dict["fee"] = buy_agg_result.get("TotalFee")

    if sell_agg_result:
        sell_dict["vol"] = sell_agg_result.get("TotalSell")
        sell_dict["fee"] = sell_agg_result.get("TotalFee")
    response_dict = {}
    response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
    response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
    # response_dict["FirstName"] = marketer.get("FirstName")
    # response_dict["LastName"] = marketer.get("LastName")

    # results.append(response_dict)

    # return response_dict
    return ResponseOut(
        result=response_dict,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )

