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
from src.schemas.marketer import (
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
    MarketerRelations,
    SearchMarketerRelations
)
from src.tools.utils import peek, to_gregorian_, marketer_entity


marketer = APIRouter(prefix="/marketer")


@marketer.get(
    "/get-marketer",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def get_marketer_profile(
    request: Request, args: MarketerIn = Depends(MarketerIn)
):
    """_summary_

    Args:
        request (Request): _description_

    Returns:
        _type_: _description_
    """
    brokerage = get_database()
    marketers_coll = brokerage["marketers"]
    results = []
    query_result = marketers_coll.find({"IdpId": args.IdpID})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketer_entity(marketers[i]))
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/marketers",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def get_marketer(request: Request):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    marketers_coll = database["marketers"]
    results = []
    query_result = marketers_coll.find({})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketer_entity(marketers[i]))
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.put(
    "/modify-marketer",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],  # , response_model=None
)
async def modify_marketer(
    request: Request, args: ModifyMarketerIn = Depends(ModifyMarketerIn)
):

    user_id = get_sub(request)

    # if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
    #     raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketer_coll = database["marketers"]

    filter = {"IdpId": args.CurrentIdpId}
    idpid = args.CurrentIdpId
    update = {"$set": {}}

    if args.FirstName is not None:
        update["$set"]["FirstName"] = args.FirstName

    if args.LastName is not None:
        update["$set"]["LastName"] = args.LastName

    if args.InvitationLink is not None:
        update["$set"]["InvitationLink"] = args.InvitationLink

    if args.RefererType is not None:
        update["$set"]["RefererType"] = args.RefererType

    if args.CreateDate is not None:
        update["$set"]["CreateDate"] = args.CreateDate

    if args.ModifiedBy is not None:
        update["$set"]["ModifiedBy"] = args.ModifiedBy

    if args.CreatedBy is not None:
        update["$set"]["CreatedBy"] = args.CreatedBy

    if args.ModifiedDate is not None:
        update["$set"]["ModifiedDate"] = args.ModifiedDate

    if args.NewIdpId is not None:
        update["$set"]["IdpId"] = args.NewIdpId
        idpid = args.NewIdpId

    if args.NationalID is not None:
        update["$set"]["Id"] = args.NationalID

    marketer_coll.update_one(filter, update)
    query_result = marketer_coll.find({"IdpId": idpid})
    marketer_dict = peek(query_result)
    return ResponseListOut(
        result=marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.put(
    "/add-marketer",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],  # , response_model=None
)
async def add_marketer(
    request: Request, args: ModifyMarketerIn = Depends(ModifyMarketerIn)
):

    user_id = get_sub(request)

    # if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
    #     raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketer_coll = database["marketers"]

    filter = {"IdpId": args.CurrentIdpId}
    update = {"$set": {}}

    if args.FirstName is not None:
        update["$set"]["FirstName"] = args.FirstName

    if args.LastName is not None:
        update["$set"]["LastName"] = args.LastName

    if args.InvitationLink is not None:
        update["$set"]["InvitationLink"] = args.InvitationLink

    if args.RefererType is not None:
        update["$set"]["RefererType"] = args.RefererType

    if args.CreateDate is not None:
        update["$set"]["CreateDate"] = args.CreateDate

    if args.ModifiedBy is not None:
        update["$set"]["ModifiedBy"] = args.ModifiedBy

    if args.CreatedBy is not None:
        update["$set"]["CreatedBy"] = args.CreatedBy

    if args.ModifiedDate is not None:
        update["$set"]["ModifiedDate"] = args.ModifiedDate

    if args.NationalID is not None:
        update["$set"]["Id"] = args.NationalID

    update["$set"]["IdpId"] = args.CurrentIdpId
    try:
        marketer_coll.insert_one(update)
    except:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error="مارکتر در دیتابیس وجود دارد.",
        )

    query_result = marketer_coll.find(filter)
    marketer_dict = peek(query_result)
    return ResponseListOut(
        result=marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/marketer-total",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
def get_marketer_total_trades(
    request: Request, args: UsersTotalPureIn = Depends(UsersTotalPureIn)
):
    # get all current marketers
    database = get_database()

    customers_coll = database["customers"]
    trades_coll = database["trades"]
    marketers_coll = database["marketers"]
    firms_coll = database["firms"]
    totals_coll = database["totals"]

    # get all marketers IdpId

    marketers_query = marketers_coll.find(
        {"IdpId": {"$exists": True, "$not": {"$size": 0}}},
        {"FirstName": 1, "LastName": 1, "_id": 0, "IdpId": 1},
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
            marketer_fullname = (
                marketer.get("FirstName") + " " + marketer.get("LastName")
            )

        # Check if customer exist
        query = {"Referer": {"$regex": marketer_fullname}}

        fields = {"PAMCode": 1}

        customers_records = customers_coll.find(query, fields)
        firms_records = firms_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records] + [
            c.get("PAMCode") for c in firms_records
        ]

        from_gregorian_date = to_gregorian_(args.from_date)

        #####################
        if not args.to_date:
            args.to_date = jd.today().date().isoformat()
        #####################

        to_gregorian_date = to_gregorian_(args.to_date)

        to_gregorian_date = datetime.strptime(
            to_gregorian_date, "%Y-%m-%d"
        ) + timedelta(days=1)
        last_month = jd.strptime(args.to_date, "%Y-%m-%d").month - 1
        if last_month < 1:
            last_month = last_month + 12
        if last_month < 10:
            last_month = "0" + str(last_month)
        last_month_str = str(jd.strptime(args.to_date, "%Y-%m-%d").year) + str(
            last_month
        )
        if last_month == 12:
            last_month_str = str(jd.strptime(args.to_date, "%Y-%m-%d").year - 1) + str(
                last_month
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

        response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
        response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
        response_dict["FirstName"] = marketer.get("FirstName")
        response_dict["LastName"] = marketer.get("LastName")
        ###########
        lmtpv = last_month_str + "TPV"
        lmtf = last_month_str + "TF"
        response_dict["LMTPV"] = totals_coll.find_one(
            {"MarketerID": marketer.get("IdpId")}
        )[lmtpv]
        response_dict["LMTF"] = totals_coll.find_one(
            {"MarketerID": marketer.get("IdpId")}
        )[lmtf]
        response_dict["UsersCount"] = customers_coll.count_documents(
            {"Referer": {"$regex": marketer_fullname}}
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
        results.sort(key=lambda x: x["UsersCount"], reverse=args.asc_desc_UC)

    # return results
    return ResponseOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/search",
    dependencies=[Depends(JWTBearer())],
    # response_model=Page[MarketerOut],
    response_model=None,
    tags=["Marketer"],
)
async def search_user_profile(
    request: Request, args: MarketersProfileIn = Depends(MarketersProfileIn)
):
    """_summary_

    Args:
        request (Request): _description_
        args (UserIn, optional): _description_. Defaults to Depends(UserIn).

    Returns:
        _type_: _description_
    """
    # get user id
    # marketer_id = get_sub(request)
    brokerage = get_database()

    # customer_coll = brokerage["customers"]
    marketer_coll = brokerage["marketers"]

    # check if marketer exists and return his name
    # query_result = marketers_coll.find({"IdpId": marketer_id})

    # marketer_dict = peek(query_result)

    # marketer_fullname = marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")

    query = {
        "$and": [
            # {"Referer": marketer_fullname},
            {"FirstName": {"$regex": args.first_name}},
            {"LastName": {"$regex": args.last_name}},
            {"CreateDate": {"$regex": args.register_date}}
            # {'Mobile': {'$regex': args.mobile}}
        ]
    }

    filter = {
        "FirstName": {"$regex": args.first_name},
        "LastName": {"$regex": args.last_name},
        "RegisterDate": {"$regex": args.register_date}
        # },
        # 'Mobile': {
        #     '$regex': args.mobile
        # }
    }
    # print(filter)
    # return paginate(marketer_coll, {})
    return paginate(marketer_coll, query, sort=[("RegisterDate", -1)])


@marketer.put(
    "/add-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],   response_model=None
)
async def add_marketers_relations(
        request: Request, args: MarketerRelations = Depends(MarketerRelations)
):
    user_id = get_sub(request)

    # if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
    #     raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketers_relations_coll = database["mrelations"]

    update = {"$set": {}}

    update["$set"]["LeaderMarketerID"] = args.LeaderMarketerID
    update["$set"]["FollowerMarketerID"] = args.FollowerMarketerID
    if args.LeaderMarketerID == args.FollowerMarketerID:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error="مارکترها نباید یکسان باشند.",
        )
    if marketers_relations_coll.find_one({"FollowerMarketerID": args.FollowerMarketerID}):
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error="این مارکتر زیرمجموعه نفر دیگری است.",
        )
    update["$set"]["CommissionCoefficient"] = args.CommissionCoefficient
    update["$set"]["UpdateDate"] = str(jd.now())
    update["$set"]["StartDate"] = str(jd.today().date())
    update["$set"]["CreateDate"] = str(jd.now())

    if args.StartDate is not None:
        update["$set"]["StartDate"] = args.StartDate
    if args.EndDate is not None:
        update["$set"]["EndDate"] = args.EndDate
        update["$set"]["GEndDate"] = to_gregorian_(args.EndDate)
    update["$set"]["GStartDate"] = to_gregorian_(args.StartDate)
    update["$set"]["GCreateDate"] = to_gregorian_(jd.strptime(update["$set"]["CreateDate"],"%Y-%m-%d %H:%M:%S.%f"))

    marketers_relations_coll.insert_one(update["$set"])

    return ResponseListOut(
        result=marketers_relations_coll.find_one({"FollowerMarketerID": args.FollowerMarketerID},{"_id":False}),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.put(
    "/modify-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],  response_model=None
)
async def modify_marketers_relations(
        request: Request, args: MarketerRelations = Depends(MarketerRelations)
):
    user_id = get_sub(request)

    # if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
    #     raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketers_relations_coll = database["mrelations"]


    update = {"$set": {}}
    update["$set"]["LeaderMarketerID"] = args.LeaderMarketerID
    update["$set"]["FollowerMarketerID"] = args.FollowerMarketerID
    if args.LeaderMarketerID == args.FollowerMarketerID:
        return ResponseListOut(
            result=[],
            timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            error="مارکترها نباید یکسان باشند.",
        )
    # if marketers_relations_coll.find_one({"FollowerMarketerID": args.FollowerMarketerID}):

    update["$set"]["CommissionCoefficient"] = args.CommissionCoefficient
    update["$set"]["UpdateDate"] = str(jd.now())
    update["$set"]["StartDate"] = str(jd.today().date())

    if args.StartDate is not None:
        update["$set"]["StartDate"] = args.StartDate

    if args.EndDate is not None:
        update["$set"]["EndDate"] = args.EndDate
        update["$set"]["GEndDate"] = to_gregorian_(args.EndDate)
    update["$set"]["GStartDate"] = to_gregorian_(args.StartDate)
    update["$set"]["GUpdateDate"] = to_gregorian_(update["$set"]["UpdateDate"])

    query = {
        "$and": [
            {"LeaderMarketerID": args.LeaderMarketerID},
            {"FollowerMarketerID": args.FollowerMarketerID}
        ]
    }
    marketers_relations_coll.update_one(query, update)
    return ResponseListOut(
        result=marketers_relations_coll.find_one({"FollowerMarketerID": args.FollowerMarketerID},{"_id":False}),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/search-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],  response_model=None
)
async def search_marketers_relations(
        request: Request, args: SearchMarketerRelations = Depends(SearchMarketerRelations)
):
    user_id = get_sub(request)

    # if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
    #     raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    from_gregorian_date = to_gregorian_(args.StartDate)
    gregorian_create_date = to_gregorian_(args.CreateDate)
    to_gregorian_date = to_gregorian_(args.EndDate)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    marketers_relations_coll = database["mrelations"]
    marketers_coll = database["marketers"]

    if args.FollowerMarketerName:
        name_query = {"$or": [
            {"FirstName": {"$regex": args.FollowerMarketerName}},
            {"LastName": {"$regex": args.FollowerMarketerName}}
        ]
        }
        fields = {"IdpId": 1}
        idps = marketers_coll.find(name_query, fields)
        codes = [c.get('IdpId') for c in idps]
        query = {
                "$and": [
                    {"TradeCode": {"$in": codes}},
                    {"StartDate": {"$gte": from_gregorian_date}},
                    {"EndDate": {"$lte": to_gregorian_date}},
                ]
            }
    elif args.LeaderMarketerName:
        name_query = {"$or": [
            {"FirstName": {"$regex": args.LeaderMarketerName}},
            {"LastName": {"$regex": args.LeaderMarketerName}}
        ]
        }
        fields = {"IdpId": 1}
        idps = marketers_coll.find(name_query, fields)
        codes = [c.get('IdpId') for c in idps]
        query = {
                "$and": [
                    {"TradeCode": {"$in": codes}},
                    {"StartDate": {"$gte": from_gregorian_date}},
                    {"EndDate": {"$lte": to_gregorian_date}},
                ]
            }



    LeaderMarketerName: str = Query("")
    FollowerMarketerName: str = Query("")
    StartDate: str = Query(default=current_date)
    EndDate: str = Query(default=current_date)
    CreateDate: str = Query(default=current_date)

    {
                "$and": [
                    {"TradeCode": {"$in": trade_codes}},
                    {"TradeDate": {"$gte": from_gregorian_date}},
                    {"TradeDate": {"$lte": to_gregorian_date}},
                ]
            }






add_pagination(marketer)


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
