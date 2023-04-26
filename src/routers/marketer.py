from fastapi import APIRouter, Depends, Request, HTTPException
from src.tools.tokens import JWTBearer, get_sub
from src.tools.database import get_database
from src.schemas.marketer import MarketerOut, ModifyMarketerIn, UsersTotalPureIn, MarketersProfileIn, MarketerIn, ConstOut, ModifyConstIn
from src.tools.utils import peek, to_gregorian_
from datetime import datetime, timedelta
from khayyam import JalaliDatetime as jd
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate


marketer = APIRouter(prefix='/marketer')

@marketer.get("/get-marketer/", dependencies=[Depends(JWTBearer())], tags=["Marketer"], response_model=Page[MarketerOut])
async def get_marketer_profile(request: Request, args: MarketerIn = Depends(MarketerIn)):
    """_summary_

    Args:
        request (Request): _description_

    Returns:
        _type_: _description_
    """
    marketer_id = args.IdpID
    brokerage = get_database()
    marketers_coll = brokerage["marketers"]
    # check if marketer exists and return his name
    return paginate(marketers_coll, {"IdpId": marketer_id})

@marketer.get("/marketers", dependencies=[Depends(JWTBearer())], tags=["Marketer"], response_model=Page[MarketerOut])
async def get_marketer(request: Request):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")
    
    database = get_database()

    marketer_coll = database["marketers"]


    return paginate(marketer_coll, {})


@marketer.put("/modify-marketer", dependencies=[Depends(JWTBearer())], tags=["Marketer"])
async def modify_marketer(request: Request, args: ModifyMarketerIn = Depends(ModifyMarketerIn)):

    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    
    database = get_database()

    marketer_coll = database["marketers"]

    filter = {"IdpId": args.CurrentIdpId}
    update = {"$set": {}}

    if args.InvitationLink != None:
        update["$set"]["InvitationLink"] = args.InvitationLink

    if args.Mobile != None:
        update["$set"]["Mobile"] = args.Mobile

    if args.FirstName != None:
        update["$set"]["FirstName"] = args.FirstName

    if args.LastName != None:
        update["$set"]["LastName"] = args.LastName

    if args.RefererType != None:
        update["$set"]["RefererType"] = args.RefererType

    if args.CreatedDate != None:
        update["$set"]["CreatedDate"] = args.CreatedDate

    if args.ModifiedBy != None:
        update["$set"]["ModifiedBy"] = args.ModifiedBy

    if args.NewIdpId != None:
        update["$set"]["NewIdpId"] = args.NewIdpId

    if args.Phone != None:
        update["$set"]["Phone"] = args.Phone

    if args.ID != None:
        update["$set"]["ID"] = args.ID

    if args.NationalID != None:
        update["$set"]["NationalID"] = args.NationalID

    modified_record = marketer_coll.update_one(filter, update)

    return modified_record.raw_result


@marketer.get("/marketer-total", dependencies=[Depends(JWTBearer())], tags=["Marketer"])
def get_marketer_total_trades(request: Request, args: UsersTotalPureIn = Depends(UsersTotalPureIn)):
    # get all current marketers
    db = get_database()

    customers_coll = db["customers"]
    trades_coll = db["trades"]
    marketers_coll = db["marketers"]
    firms_coll = db["firms"]
    totals_coll = db["totals"]

    # get all marketers IdpId

    marketers_query = marketers_coll.find(
        {"IdpId": { "$exists": True, "$not": {"$size": 0} } }, 
        {"FirstName": 1, "LastName": 1 ,"_id": 0,"IdpId":1}
        )
    marketers_list = list(marketers_query)

    results = []
    for marketer in marketers_list:
        response_dict = {}
        if marketer.get("FirstName") == "":
            marketer_fullname = marketer.get("LastName")
        elif marketer.get("LastName") == "":
            marketer_fullname = marketer.get("FirstName")
        else:
            marketer_fullname = marketer.get("FirstName") + " " + marketer.get("LastName")


        # Check if customer exist
        query = {"Referer": {"$regex": marketer_fullname}} 

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        trade_codes = [c.get('PAMCode') for c in customers_records] + [c.get('PAMCode') for c in firms_records]

        from_gregorian_date = to_gregorian_(args.from_date)

#####################
        if not args.to_date: args.to_date = jd.today().date().isoformat()
#####################

        to_gregorian_date = to_gregorian_(args.to_date)

        to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(days=1)
        last_month=jd.strptime(args.to_date, '%Y-%m-%d').month - 1
        if last_month < 1: last_month = last_month + 12
        if last_month < 10: last_month = "0"+str(last_month)
        last_month_str = str(jd.strptime(args.to_date, '%Y-%m-%d').year) + str(last_month)
        if last_month ==12: last_month_str = str(jd.strptime(args.to_date, '%Y-%m-%d').year - 1) + str(last_month)
        to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")



        buy_pipeline = [
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}}, 
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}},
                        {"TradeType": 1}
                        ]
                    }
                },
            {
                "$project": {
                    "Price": 1,
                    "Volume": 1,
                    "Total" : {"$multiply": ["$Price", "$Volume"]},
                    "TotalCommission": 1,
                    "TradeItemBroker": 1,
                    "Buy": {
                        "$add": [
                            "$TotalCommission",
                            {"$multiply": ["$Price", "$Volume"]}
                            ]
                        }
                }
            },
            {
                "$group": {
                    "_id": "$id", 
                    "TotalFee": {
                        "$sum": "$TradeItemBroker"
                    },
                    "TotalBuy": {
                        "$sum": "$Buy"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TotalBuy": 1,
                    "TotalFee": 1
                }
            }
        ]

        sell_pipeline = [ 
            {
                "$match": {
                    "$and": [
                        {"TradeCode": {"$in": trade_codes}}, 
                        {"TradeDate": {"$gte": from_gregorian_date}},
                        {"TradeDate": {"$lte": to_gregorian_date}},
                        {"TradeType": 2}
                        ]
                    }
                },
            {
                "$project": {
                    "Price": 1,
                    "Volume": 1,
                    "Total" : {"$multiply": ["$Price", "$Volume"]},
                    "TotalCommission": 1,
                    "TradeItemBroker": 1,
                    "Sell": {
                        "$subtract": [
                            {"$multiply": ["$Price", "$Volume"]},
                            "$TotalCommission"
                            ]
                        }
                }
            },
            {
                "$group": {
                    "_id": "$id", 
                    "TotalFee": {
                     "$sum": "$TradeItemBroker"
                    },
                    "TotalSell": {
                        "$sum": "$Sell"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "TotalSell": 1,
                    "TotalFee": 1
                }
            }
        ]

        buy_agg_result = peek(trades_coll.aggregate(pipeline=buy_pipeline))
        sell_agg_result = peek(trades_coll.aggregate(pipeline=sell_pipeline))

        buy_dict = {
            "vol": 0,
            "fee": 0
        }

        sell_dict = {
            "vol": 0,
            "fee": 0
        }

        if buy_agg_result:
            buy_dict['vol'] = buy_agg_result.get("TotalBuy")
            buy_dict['fee'] = buy_agg_result.get("TotalFee")

        if sell_agg_result:
            sell_dict['vol'] = sell_agg_result.get("TotalSell")
            sell_dict['fee'] = sell_agg_result.get("TotalFee")

        response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
        response_dict["FirstName"] = marketer.get("FirstName")
        response_dict["LastName"] = marketer.get("LastName")
###########
        lmtpv = last_month_str + "TPV"
        lmtf = last_month_str + "TF"
        response_dict["LMTPV"] = totals_coll.find_one({
            'MarketerID': marketer.get("IdpId")})[lmtpv]
        response_dict["LMTF"] = totals_coll.find_one({
            'MarketerID': marketer.get("IdpId")})[lmtf]
        response_dict["UsersCount"] = customers_coll.count_documents({"Referer": {"$regex": marketer_fullname}})
###########
        results.append(response_dict)
    if args.sorted:
        results.sort(key=lambda x: x["TotalFee"], reverse=args.asc_desc_TF)
        results.sort(key=lambda x: x["TotalPureVolume"], reverse=args.asc_desc_TPV)
        results.sort(key=lambda x: x["LMTF"], reverse=args.asc_desc_LMTF)
        results.sort(key=lambda x: x["LMTPV"], reverse=args.asc_desc_LMTPV)
        results.sort(key=lambda x: x["FirstName"], reverse=args.asc_desc_FN)
        results.sort(key=lambda x: x["LastName"], reverse=args.asc_desc_LN)
        results.sort(key=lambda x: x["UsersCount"], reverse=args.asc_desc_UC)

    return results



@marketer.get("/search/", dependencies=[Depends(JWTBearer())], response_model=Page[MarketerOut], tags=["Marketer"])
async def search_user_profile(request: Request, args: MarketersProfileIn = Depends(MarketersProfileIn)):
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

    # customer_coll = brokerage["customers"]
    marketer_coll = brokerage["marketers"]

    # check if marketer exists and return his name
    # query_result = marketers_coll.find({"IdpId": marketer_id})

    # marketer_dict = peek(query_result)

    # marketer_fullname = marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")

    query = {"$and": [
        # {"Referer": marketer_fullname},
        {"FirstName": {"$regex": args.first_name}},
        {"LastName": {"$regex": args.last_name}},
        {'CreateDate': {'$regex': args.register_date}}
        # {'Mobile': {'$regex': args.mobile}}
        ]
    }

    filter = {
        'FirstName': {
            '$regex': args.first_name
        },
        'LastName': {
            '$regex': args.last_name
        },
        'RegisterDate': {
            '$regex': args.register_date
        }
        # },
        # 'Mobile': {
        #     '$regex': args.mobile
        # }
    }
    print(filter)
    # return paginate(marketer_coll, {})
    return paginate(marketer_coll, query, sort=[("RegisterDate", -1)])


@marketer.get("/get-factor-consts/", dependencies=[Depends(JWTBearer())], tags=["Factor"], response_model=Page[ConstOut])
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
    # check if marketer exists and return his name
    q=consts_coll.find_one({"MarketerID": marketer_id})
    return paginate(consts_coll, {"MarketerID": marketer_id})
    # return q


@marketer.get("/get-all-factor-consts", dependencies=[Depends(JWTBearer())], tags=["Factor"], response_model=Page[ConstOut])
async def get_all_factors_consts(request: Request):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    consts_coll = database["consts"]

    return paginate(consts_coll, {})


@marketer.put("/modify-factor-consts", dependencies=[Depends(JWTBearer())], tags=["Factor"])
async def modify_factor_consts(request: Request, args: ModifyConstIn = Depends(ModifyConstIn)):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    consts_coll = database["consts"]

    filter = {"IdpId": args.MarketerID}
    update = {"$set": {}}

    if args.FixIncome != None:
        update["$set"]["FixIncome"] = args.FixIncome

    if args.Insurance != None:
        update["$set"]["Insurance"] = args.Insurance

    if args.Collateral != None:
        update["$set"]["Collateral"] = args.Collateral

    if args.Tax != None:
        update["$set"]["Tax"] = args.Tax

    modified_record = consts_coll.update_one(filter, update)

    return modified_record.raw_result


add_pagination(marketer)




def totaliter(marketer_fullname,from_gregorian_date,to_gregorian_date):
    db = get_database()

    customers_coll = db["customers"]
    trades_coll = db["trades"]
    marketers_coll = db["marketers"]
    firms_coll = db["firms"]

    query = {"Referer": {"$regex": marketer_fullname}}

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    firms_records = firms_coll.find(query, fields)
    trade_codes = [c.get('PAMCode') for c in customers_records] + [c.get('PAMCode') for c in firms_records]


    buy_pipeline = [
        {
            "$match": {
                "$and": [
                    {"TradeCode": {"$in": trade_codes}},
                    {"TradeDate": {"$gte": from_gregorian_date}},
                    {"TradeDate": {"$lte": to_gregorian_date}},
                    {"TradeType": 1}
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
                        {"$multiply": ["$Price", "$Volume"]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$id",
                "TotalFee": {
                    "$sum": "$TradeItemBroker"
                },
                "TotalBuy": {
                    "$sum": "$Buy"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "TotalBuy": 1,
                "TotalFee": 1
            }
        }
    ]
    sell_pipeline = [
        {
            "$match": {
                "$and": [
                    {"TradeCode": {"$in": trade_codes}},
                    {"TradeDate": {"$gte": from_gregorian_date}},
                    {"TradeDate": {"$lte": to_gregorian_date}},
                    {"TradeType": 2}
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
                        "$TotalCommission"
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$id",
                "TotalFee": {
                    "$sum": "$TradeItemBroker"
                },
                "TotalSell": {
                    "$sum": "$Sell"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "TotalSell": 1,
                "TotalFee": 1
            }
        }
    ]
    buy_agg_result = peek(trades_coll.aggregate(pipeline=buy_pipeline))
    sell_agg_result = peek(trades_coll.aggregate(pipeline=sell_pipeline))

    buy_dict = {
        "vol": 0,
        "fee": 0
    }

    sell_dict = {
        "vol": 0,
        "fee": 0
    }

    if buy_agg_result:
        buy_dict['vol'] = buy_agg_result.get("TotalBuy")
        buy_dict['fee'] = buy_agg_result.get("TotalFee")

    if sell_agg_result:
        sell_dict['vol'] = sell_agg_result.get("TotalSell")
        sell_dict['fee'] = sell_agg_result.get("TotalFee")
    response_dict = {}
    response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
    response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
    # response_dict["FirstName"] = marketer.get("FirstName")
    # response_dict["LastName"] = marketer.get("LastName")

    # results.append(response_dict)

    return response_dict
