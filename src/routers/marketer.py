"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.tools.tokens import JWTBearer, get_role_permission
from src.tools.database import get_database
from src.schemas.marketer import (
    ModifyMarketerIn,
    AddMarketerIn,
    UsersTotalPureIn,
    MarketersProfileIn,
    MarketerIn,
    DiffTradesIn,
    ResponseOut,
    ResponseListOut,
    MarketerRelations,
    DelMarketerRelations,
    SearchMarketerRelations,
)
from src.tools.utils import (
    peek,
    to_gregorian_,
    marketer_entity,
    get_marketer_name,
    check_permissions,
)


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
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    brokerage = get_database()
    marketers_coll = brokerage["marketers"]
    if args.IdpID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )
    query_result = marketers_coll.find_one({"IdpId": args.IdpID}, {"_id": False})
    if not query_result:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "موردی با IDP داده شده یافت نشد.",
                "code": "30004",
            },
        }
        return JSONResponse(status_code=404, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "موردی با IDP داده شده یافت نشد.",
        #         "code": "30004",
        #     },
        # )
    return ResponseListOut(
        result=query_result,
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
    """_summary_

    Args:
        request (Request): _description_

    Raises:
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    marketers_coll = database["marketers"]
    results = []
    query_result = marketers_coll.find({})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketer_entity(marketers[i]))
    if not results:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "موردی در دیتابیس یافت نشد.",
                "code": "30001",
            },
        }
        return JSONResponse(status_code=404, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # )
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
    request: Request,
    mmi: ModifyMarketerIn,
):
    """_summary_

    Args:
        request (Request): _description_
        args (ModifyMarketerIn, optional): _description_. Defaults to Depends(ModifyMarketerIn).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Write",
        "MarketerAdmin.Marketer.Update",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    marketer_coll = database["marketers"]
    admins_coll = database["factors"]
    if mmi.CurrentIdpId is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )
    filter = {"IdpId": mmi.CurrentIdpId}
    idpid = mmi.CurrentIdpId
    update = {"$set": {}}

    if mmi.FirstName is not None:
        update["$set"]["FirstName"] = mmi.FirstName

    if mmi.LastName is not None:
        update["$set"]["LastName"] = mmi.LastName

    if mmi.InvitationLink is not None:
        update["$set"]["InvitationLink"] = mmi.InvitationLink

    if mmi.RefererType is not None:
        update["$set"]["RefererType"] = mmi.RefererType

    # Let Super Admin can change CreatedDate
    if check_permissions(
        role_perm["MarketerAdmin"],
        ["MarketerAdmin.All.All", "MarketerAdmin.Marketer.All"],
    ):
        if mmi.CreateDate is not None:
            update["$set"]["CreateDate"] = mmi.CreateDate

    if mmi.ModifiedBy is not None:
        update["$set"]["ModifiedBy"] = admins_coll.find_one(
            {"IdpId": user_id}, {"_id": False}
        ).get("FullName")

    # Let Super Admin can change CreatedBy
    if check_permissions(
        role_perm["MarketerAdmin"],
        ["MarketerAdmin.All.All", "MarketerAdmin.Marketer.All"],
    ):
        if mmi.CreatedBy is not None:
            update["$set"]["CreatedBy"] = mmi.CreatedBy

    update["$set"]["ModifiedDate"] = jd.today().strftime("%Y-%m-%d")

    if mmi.NewIdpId is not None:
        update["$set"]["IdpId"] = mmi.NewIdpId
        idpid = mmi.NewIdpId

    if mmi.NationalID is not None:
        try:
            ddd = int(mmi.NationalID)
            update["$set"]["Id"] = mmi.NationalID
        except:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "کد ملی را درست وارد کنید.",
                    "code": "30066",
                },
            }
            return JSONResponse(status_code=412, content=resp)

            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "کد ملی را درست وارد کنید.",
            #         "code": "30066",
            #     },
            # )

    marketer_coll.update_one(filter, update)
    query_result = marketer_coll.find_one({"IdpId": idpid}, {"_id": False})
    if not query_result:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        }
        return JSONResponse(status_code=404, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # )
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.post(
    "/add-marketer",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
)
async def add_marketer(
    request: Request, ami: AddMarketerIn
):
    """_summary_

    Args:
        request (Request): _description_
        args (AddMarketerIn, optional): _description_. Defaults to Depends(AddMarketerIn).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Create",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    admins_coll = database["factors"]
    marketer_coll = database["marketers"]
    if ami.CurrentIdpId is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    filter = {"IdpId": ami.CurrentIdpId}
    update = {"$set": {}}

    if ami.FirstName is not None:
        update["$set"]["FirstName"] = ami.FirstName

    if ami.LastName is not None:
        update["$set"]["LastName"] = ami.LastName

    if ami.InvitationLink is not None:
        update["$set"]["InvitationLink"] = ami.InvitationLink

    if ami.RefererType is not None:
        update["$set"]["RefererType"] = ami.RefererType

    update["$set"]["CreateDate"] = jd.today().strftime("%Y-%m-%d")
    update["$set"]["ModifiedBy"] = admins_coll.find_one(
        {"IdpID": user_id}, {"_id": False}
    ).get("FullName")
    update["$set"]["CreatedBy"] = admins_coll.find_one(
        {"IdpID": user_id}, {"_id": False}
    ).get("FullName")
    update["$set"]["ModifiedDate"] = jd.today().strftime("%Y-%m-%d")

    if ami.NationalID is not None:
        try:
            ddd = int(ami.NationalID)
            update["$set"]["Id"] = ami.NationalID
        except:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "کد ملی را درست وارد کنید.",
                    "code": "30066",
                },
            }
            return JSONResponse(status_code=412, content=resp)
            #
            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "کد ملی را درست وارد کنید.",
            #         "code": "30066",
            #     },
            # )

    update["$set"]["IdpId"] = ami.CurrentIdpId
    try:
        marketer_coll.insert_one(update["$set"])
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "مارکتر در دیتابیس وجود دارد.",
                "code": "30006",
            },
        }
        return JSONResponse(status_code=409, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "مارکتر در دیتابیس وجود دارد.",
        #         "code": "30006",
        #     },
        # )

    query_result = marketer_coll.find_one(filter, {"_id": False})
    if not query_result:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "ورودی ها را دوباره چک کنید.", "code": "30051"},
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "ورودی ها را دوباره چک کنید.", "code": "30051"},
        # )
    return ResponseListOut(
        result=query_result,
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
    """_summary_

    Args:
        request (Request): _description_
        args (UsersTotalPureIn, optional): _description_. Defaults to Depends(UsersTotalPureIn).

    Returns:
        _type_: _description_
    """
    # get all current marketers
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

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
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    brokerage = get_database()
    marketer_coll = brokerage["marketers"]
    query = {
        "$and": [
            {"FirstName": {"$regex": args.first_name}},
            {"LastName": {"$regex": args.last_name}},
            {"CreateDate": {"$regex": args.register_date}},
        ]
    }

    filter = {
        "FirstName": {"$regex": args.first_name},
        "LastName": {"$regex": args.last_name},
        "RegisterDate": {"$regex": args.register_date},
    }
    results = []
    # query_result = marketer_coll.find({"IdpId": args.IdpID},{"_id":False})
    try:
        query_result = marketer_coll.find_one(query, {"_id": False})
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "ورودی نام غیرقابل قبول است.", "code": "30050"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "ورودی نام غیرقابل قبول است.", "code": "30050"},
        # )

    query_result = marketer_coll.find(query, {"_id": False})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketer_entity(marketers[i]))
    if not results:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "موردی با متغیرهای داده شده یافت نشد.",
                "code": "30008",
            },
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "موردی با متغیرهای داده شده یافت نشد.",
        #         "code": "30008",
        #     },
        # )
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.post(
    "/add-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def add_marketers_relations(
    request: Request,
    mrel: MarketerRelations,
):
    """_summary_

    Args:
        request (Request): _description_
        args (MarketerRelations, optional): _description_. Defaults to Depends(MarketerRelations).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Create",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketers_relations_coll = database["mrelations"]
    marketers_coll = database["marketers"]
    if mrel.LeaderMarketerID and mrel.FollowerMarketerID:
        pass
    else:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکترها را وارد کنید.", "code": "30009"},
        }
        return JSONResponse(status_code=412, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکترها را وارد کنید.", "code": "30009"},
        # )
    try:
        d = int(mrel.CommissionCoefficient)
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "کمیسیون را وارد کنید.", "code": "30010"},
        }
        return JSONResponse(status_code=412, content=resp)

        # if args.CommissionCoefficient is None:
        # if d is None:
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "کمیسیون را وارد کنید.", "code": "30010"},
        # )
    update = {"$set": {}}

    update["$set"]["LeaderMarketerID"] = mrel.LeaderMarketerID
    update["$set"]["FollowerMarketerID"] = mrel.FollowerMarketerID
    if mrel.LeaderMarketerID == mrel.FollowerMarketerID:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "مارکترها نباید یکسان باشند.", "code": "30011"},
        }
        return JSONResponse(status_code=409, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "مارکترها نباید یکسان باشند.", "code": "30011"},
        # )
    if marketers_relations_coll.find_one(
        {"FollowerMarketerID": mrel.FollowerMarketerID}
    ):
        if marketers_relations_coll.find_one(
            {"LeaderMarketerID": mrel.LeaderMarketerID}
        ):
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "این ارتباط وجود دارد.",
                    "code": "30072",
                },
            }
            return JSONResponse(status_code=409, content=resp)

            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "این ارتباط وجود دارد.",
            #         "code": "30072",
            #     },
            # )

        else:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "این مارکتر زیرمجموعه نفر دیگری است.",
                    "code": "30012",
                },
            }
            return JSONResponse(status_code=406, content=resp)
            #
            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "این مارکتر زیرمجموعه نفر دیگری است.",
            #         "code": "30012",
            #     },
            # )
    try:
        d = int(mrel.CommissionCoefficient)
        if 0 < mrel.CommissionCoefficient < 1:
            update["$set"]["CommissionCoefficient"] = mrel.CommissionCoefficient
        else:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "کمیسیون را به درستی وارد کنید.",
                    "code": "30010",
                },
            }
            return JSONResponse(status_code=412, content=resp)

            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "کمیسیون را به درستی وارد کنید.",
            #         "code": "30010",
            #     },
            # )
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "کمیسیون را به درستی وارد کنید.",
                "code": "30010",
            },
        }
        return JSONResponse(status_code=412, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "کمیسیون را به درستی وارد کنید.",
        #         "code": "30010",
        #     },
        # )

    update["$set"]["CreateDate"] = str(jd.now())
    update["$set"]["UpdateDate"] = update["$set"]["CreateDate"]
    update["$set"]["StartDate"] = str(jd.today().date())

    if mrel.StartDate is not None:
        update["$set"]["StartDate"] = mrel.StartDate
    if mrel.EndDate is not None:
        update["$set"]["EndDate"] = mrel.EndDate
        try:
            update["$set"]["GEndDate"] = jd.strptime(
                update["$set"]["EndDate"], "%Y-%m-%d"
            ).todatetime()
        except:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "تاریخ انتها را درست وارد کنید.",
                    "code": "30010",
                },
            }
            return JSONResponse(status_code=412, content=resp)

            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "تاریخ انتها را درست وارد کنید.",
            #         "code": "30010",
            #     },
            # )

    else:
        update["$set"]["GEndDate"] = jd.strptime("1500-12-29", "%Y-%m-%d").todatetime()
    try:
        update["$set"]["GStartDate"] = jd.strptime(
            update["$set"]["StartDate"], "%Y-%m-%d"
        ).todatetime()
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "تاریخ ابتدا را درست وارد کنید.",
                "code": "30010",
            },
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "تاریخ ابتدا را درست وارد کنید.",
        #         "code": "30010",
        #     },
        # )

    update["$set"]["GCreateDate"] = jd.strptime(
        update["$set"]["CreateDate"], "%Y-%m-%d %H:%M:%S.%f"
    ).todatetime()
    update["$set"]["GUpdateDate"] = jd.strptime(
        update["$set"]["UpdateDate"], "%Y-%m-%d %H:%M:%S.%f"
    ).todatetime()
    if marketers_coll.find_one(
        {"IdpId": mrel.FollowerMarketerID}
    ) and marketers_coll.find_one({"IdpId": mrel.LeaderMarketerID}):
        pass
    else:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "مارکتری با ID داده شده وجود ندارد.",
                "code": "30010",
            },
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "مارکتری با ID داده شده وجود ندارد.",
        #         "code": "30010",
        #     },
        # )

    update["$set"]["FollowerMarketerName"] = get_marketer_name(
        marketers_coll.find_one({"IdpId": mrel.FollowerMarketerID})
    )
    update["$set"]["LeaderMarketerName"] = get_marketer_name(
        marketers_coll.find_one({"IdpId": mrel.LeaderMarketerID})
    )

    marketers_relations_coll.insert_one(update["$set"])

    return ResponseListOut(
        result=marketers_relations_coll.find_one(
            {"FollowerMarketerID": mrel.FollowerMarketerID}, {"_id": False}
        ),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.put(
    "/modify-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def modify_marketers_relations(
    request: Request,
    mrel: MarketerRelations,
):
    """_summary_

    Args:
        request (Request): _description_
        args (MarketerRelations, optional): _description_. Defaults to Depends(MarketerRelations).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Write",
        "MarketerAdmin.Marketer.Update",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketers_relations_coll = database["mrelations"]
    if mrel.LeaderMarketerID and mrel.FollowerMarketerID:
        pass
    else:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکترها را وارد کنید.", "code": "30009"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکترها را وارد کنید.", "code": "30009"},
        # )
    query = {
        "$and": [
            {"LeaderMarketerID": mrel.LeaderMarketerID},
            {"FollowerMarketerID": mrel.FollowerMarketerID},
        ]
    }
    if marketers_relations_coll.find_one(query) is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "این رابطه وجود ندارد.", "code": "30011"},
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "این رابطه وجود ندارد.", "code": "30011"},
        # )

    if mrel.CommissionCoefficient is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "کمیسیون را وارد کنید.", "code": "30010"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "کمیسیون را وارد کنید.", "code": "30010"},
        # )

    update = {"$set": {}}
    try:
        d = int(mrel.CommissionCoefficient)
        if 0 < mrel.CommissionCoefficient < 1:
            update["$set"]["CommissionCoefficient"] = mrel.CommissionCoefficient
        else:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "کمیسیون را به درستی وارد کنید.",
                    "code": "30010",
                },
            }
            return JSONResponse(status_code=412, content=resp)
            #
            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "کمیسیون را به درستی وارد کنید.",
            #         "code": "30010",
            #     },
            # )
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "کمیسیون را به درستی وارد کنید.",
                "code": "30010",
            },
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "کمیسیون را به درستی وارد کنید.",
        #         "code": "30010",
        #     },
        # )

    update["$set"]["LeaderMarketerID"] = mrel.LeaderMarketerID
    update["$set"]["FollowerMarketerID"] = mrel.FollowerMarketerID
    if mrel.LeaderMarketerID == mrel.FollowerMarketerID:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "مارکترها نباید یکسان باشند.", "code": "30011"},
        }
        return JSONResponse(status_code=409, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "مارکترها نباید یکسان باشند.", "code": "30011"},
        # )
    # if marketers_relations_coll.find_one({"FollowerMarketerID": args.FollowerMarketerID}):
    update["$set"]["CommissionCoefficient"] = mrel.CommissionCoefficient
    update["$set"]["UpdateDate"] = str(jd.now())
    update["$set"]["StartDate"] = str(jd.today().date())

    if mrel.StartDate is not None:
        update["$set"]["StartDate"] = mrel.StartDate
        try:
            update["$set"]["GStartDate"] = jd.strptime(
                update["$set"]["StartDate"], "%Y-%m-%d"
            ).todatetime()
        except:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "تاریخ ابتدا را درست وارد کنید.",
                    "code": "30010",
                },
            }
            return JSONResponse(status_code=412, content=resp)

    if mrel.EndDate is not None:
        update["$set"]["EndDate"] = mrel.EndDate
        try:
            update["$set"]["GEndDate"] = jd.strptime(
                update["$set"]["EndDate"], "%Y-%m-%d"
            ).todatetime()
        except:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "تاریخ انتها را درست وارد کنید.",
                    "code": "30010",
                },
            }
            return JSONResponse(status_code=412, content=resp)

        if update["$set"]["GEndDate"] < update["$set"]["GStartDate"]:
            resp = {
                "result": [],
                "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "error": {
                    "message": "تاریخ پایان قبل از شروع است.",
                    "code": "30071",
                },
            }
            return JSONResponse(status_code=400, content=resp)
            #
            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "تاریخ پایان قبل از شروع است.",
            #         "code": "30071",
            #     },
            # )

    update["$set"]["GUpdateDate"] = jd.strptime(
        update["$set"]["UpdateDate"], "%Y-%m-%d %H:%M:%S.%f"
    ).todatetime()

    query = {
        "$and": [
            {"LeaderMarketerID": mrel.LeaderMarketerID},
            {"FollowerMarketerID": mrel.FollowerMarketerID},
        ]
    }
    marketers_relations_coll.update_one(query, update)
    return ResponseListOut(
        result=marketers_relations_coll.find_one(
            {"FollowerMarketerID": mrel.FollowerMarketerID}, {"_id": False}
        ),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/search-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def search_marketers_relations(
    request: Request, args: SearchMarketerRelations = Depends(SearchMarketerRelations)
):
    """_summary_

    Args:
        request (Request): _description_
        args (SearchMarketerRelations, optional): _description_.
        Defaults to Depends(SearchMarketerRelations).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    from_gregorian_date = jd.strptime(args.StartDate, "%Y-%m-%d").todatetime()
    to_gregorian_date = jd.strptime(args.EndDate, "%Y-%m-%d").todatetime() + timedelta(
        days=1
    )

    marketers_relations_coll = database["mrelations"]
    # marketers_coll = database["marketers"]
    query = {}
    if args.FollowerMarketerName or args.FollowerMarketerID:
        if args.FollowerMarketerName is None:
            args.FollowerMarketerName = ""
        if args.LeaderMarketerName is None:
            args.LeaderMarketerName = ""
        if args.LeaderMarketerID is None:
            args.LeaderMarketerID = ""
        query = {
            "$and": [
                {"LeaderMarketerID": {"$regex": args.LeaderMarketerID}},
                {"FollowerMarketerName": {"$regex": args.FollowerMarketerName}},
                {"LeaderMarketerName": {"$regex": args.LeaderMarketerName}},
                {"GStartDate": {"$gte": from_gregorian_date}},
                {"GEndDate": {"$lte": to_gregorian_date}},
            ]
        }
        if args.FollowerMarketerID:
            query = {
                "$and": [
                    {"FollowerMarketerID": args.FollowerMarketerID},
                    {"LeaderMarketerID": {"$regex": args.LeaderMarketerID}},
                    {"FollowerMarketerName": {"$regex": args.FollowerMarketerName}},
                    {"LeaderMarketerName": {"$regex": args.LeaderMarketerName}},
                    {"GStartDate": {"$gte": from_gregorian_date}},
                    {"GEndDate": {"$lte": to_gregorian_date}},
                ]
            }
    elif args.LeaderMarketerName or args.LeaderMarketerID:
        if args.LeaderMarketerName is None:
            args.LeaderMarketerName = ""
        if args.FollowerMarketerName is None:
            args.FollowerMarketerName = ""
        if args.FollowerMarketerID is None:
            args.FollowerMarketerID = ""
        query = {
            "$and": [
                {"FollowerMarketerID": {"$regex": args.FollowerMarketerID}},
                {"FollowerMarketerName": {"$regex": args.FollowerMarketerName}},
                {"LeaderMarketerName": {"$regex": args.LeaderMarketerName}},
                {"GStartDate": {"$gte": from_gregorian_date}},
                {"GEndDate": {"$lte": to_gregorian_date}},
            ]
        }
        if args.LeaderMarketerID:
            query = {
                "$and": [
                    {"FollowerMarketerID": {"$regex": args.FollowerMarketerID}},
                    {"LeaderMarketerID": args.LeaderMarketerID},
                    {"FollowerMarketerName": {"$regex": args.FollowerMarketerName}},
                    {"LeaderMarketerName": {"$regex": args.LeaderMarketerName}},
                    {"GStartDate": {"$gte": from_gregorian_date}},
                    {"GEndDate": {"$lte": to_gregorian_date}},
                ]
            }
    results = []
    try:
        query_result = marketers_relations_coll.find_one(query, {"_id": False})
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "ورودی نام غیرقابل قبول است.", "code": "30050"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "ورودی نام غیرقابل قبول است.", "code": "30050"},
        # )

    query_result = marketers_relations_coll.find(query, {"_id": False})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketers[i])
    if not results:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "موردی برای متغیرهای داده شده یافت نشد.",
                "code": "30003",
            },
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "موردی برای متغیرهای داده شده یافت نشد.",
        #         "code": "30003",
        #     },
        # )
    result = {}
    result["code"] = "Null"
    result["message"] = "Null"
    result["totalCount"] = len(marketers)
    result["pagedData"] = results
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="null",
    )


@marketer.delete(
    "/delete-marketers-relations",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def delete_marketers_relations(
    request: Request, args: DelMarketerRelations = Depends(DelMarketerRelations)
):
    """_summary_

    Args:
        request (Request): _description_
        args (DelMarketerRelations, optional): _description_.
            Defaults to Depends(DelMarketerRelations).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Delete",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    marketers_relations_coll = database["mrelations"]
    marketers_coll = database["marketers"]
    if args.LeaderMarketerID and args.FollowerMarketerID:
        pass
    else:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکترها را وارد کنید.", "code": "30009"},
        }
        return JSONResponse(status_code=400, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکترها را وارد کنید.", "code": "30009"},
        # )
    if args.LeaderMarketerID == args.FollowerMarketerID:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "مارکترها نباید یکسان باشند.", "code": "30011"},
        }
        return JSONResponse(status_code=409, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "مارکترها نباید یکسان باشند.", "code": "30011"},
        # )
    q = marketers_relations_coll.find_one(
        {"FollowerMarketerID": args.FollowerMarketerID}, {"_id": False}
    )

    if not q:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "این مارکتر زیرمجموعه کسی نیست.",
                "code": "30052",
            },
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "این مارکتر زیرمجموعه کسی نیست.",
        #         "code": "30052",
        #     },
        # )
    if not q.get("LeaderMarketerID") == args.LeaderMarketerID:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "این مارکتر زیرمجموعه نفر دیگری است.",
                "code": "30012",
            },
        }
        return JSONResponse(status_code=409, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "این مارکتر زیرمجموعه نفر دیگری است.",
        #         "code": "30012",
        #     },
        # )

    results = []
    FollowerMarketerName = get_marketer_name(
        marketers_coll.find_one({"IdpId": args.FollowerMarketerID})
    )
    LeaderMarketerName = get_marketer_name(
        marketers_coll.find_one({"IdpId": args.LeaderMarketerID})
    )

    qqq = marketers_relations_coll.find_one(
        {"FollowerMarketerID": args.FollowerMarketerID}, {"_id": False}
    )
    results.append(qqq)
    results.append(
        {"MSG": f"ارتباط بین {LeaderMarketerName} و{FollowerMarketerName} برداشته شد."}
    )
    marketers_relations_coll.delete_one({"FollowerMarketerID": args.FollowerMarketerID})
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/users-diff-marketer",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"],
    response_model=None,
)
async def users_diff_with_tbs(
    request: Request, args: DiffTradesIn = Depends(DiffTradesIn)
):
    """_summary_

    Args:
        request (Request): _description_
        args (DiffTradesIn, optional): _description_. Defaults to Depends(DiffTradesIn).

    Returns:
        _type_: _description_
    """
    role_perm = get_role_permission(request)
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["MarketerAdmin"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()

    customers_coll = database["customers"]
    firms_coll = database["firms"]
    # trades_coll = database["trades"]
    marketers_coll = database["marketers"]
    if args.IdpID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    # check if marketer exists and return his name
    query_result = marketers_coll.find({"IdpId": args.IdpID})

    marketer_dict = peek(query_result)

    marketer_fullname = get_marketer_name(marketer_dict)
    # Check if customer exist

    customers_records = customers_coll.find(
        {"Referer": marketer_fullname}, {"PAMCode": 1}
    )

    firms_records = firms_coll.find({"Referer": marketer_fullname}, {"PAMCode": 1})

    trade_codes = [c.get("PAMCode") for c in customers_records] + [
        c.get("PAMCode") for c in firms_records
    ]
    ###############
    start_date = jd.strptime(args.from_date, "%Y-%m-%d")
    end_date = jd.strptime(args.to_date, "%Y-%m-%d")

    delta = timedelta(days=1)
    dates = []
    while start_date < end_date:
        # add current date to list by converting  it to iso format
        dates.append(str(start_date.date()))
        # increment start date by timedelta
        start_date += delta

    ###############
    result = []
    for date in dates:
        for trade_code in trade_codes:
            q = bs_calculator(trade_code, date)
            if q["BuyDiff"] == 0 and q["SellDiff"] == 0:
                pass
            else:
                result.append(q)
    if not result:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "مغایرتی در تاریخ های داده شده مشاهده نشد.",
                "code": "30013",
            },
        }
        return JSONResponse(status_code=404, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "مغایرتی در تاریخ های داده شده مشاهده نشد.",
        #         "code": "30013",
        #     },
        # )
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(marketer)


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


def totaliter(marketer_fullname, from_gregorian_date, to_gregorian_date):
    """_summary_

    Args:
        marketer_fullname (_type_): _description_
        from_gregorian_date (_type_): _description_
        to_gregorian_date (_type_): _description_

    Returns:
        _type_: _description_
    """
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
    return response_dict


def bs_calculator(trade_code, date, page=1, size=10):
    """_summary_

    Args:
        trade_code (_type_): _description_
        date (_type_): _description_
        page (int, optional): _description_. Defaults to 1.
        size (int, optional): _description_. Defaults to 10.

    Returns:
        _type_: _description_
    """
    database = get_database()
    trades_coll = database["trades"]
    customers_coll = database["customers"]
    firms_coll = database["firms"]

    commisions_coll = database["commisions"]
    gdate = to_gregorian_(date)

    from_gregorian_date = to_gregorian_(date)
    to_gregorian_date = to_gregorian_(date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "$and": [
                    {"TradeCode": trade_code},
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
                "Buy": 1,
                "Sell": 1,
                "TradeItemBroker": 1,
                "TradeCode": 1,
                "Buy": {
                    "$cond": {
                        "if": {"$eq": ["$TradeType", 1]},
                        "then": "$TradeItemBroker",
                        "else": 0,
                    }
                },
                "Sell": {
                    "$cond": {
                        "if": {"$ne": ["$TradeType", 1]},
                        "then": "$TradeItemBroker",
                        "else": 0,
                    }
                },
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
                "TotalBuy": {"$sum": "$Buy"},
                "TotalSell": {"$sum": "$Sell"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "TradeCode": "$_id",
                "TotalPureVolume": 1,
                "TotalFee": 1,
                "TotalBuy": 1,
                "TotalSell": 1,
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
                "TotalBuy": 1,
                "TotalSell": 1,
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
    # customer = {}
    cus_dict = {}
    bbb = customers_coll.find_one({"PAMCode": trade_code}, {"_id": False})
    if bbb:
        cus_dict["TradeCode"] = trade_code
        cus_dict["LedgerCode"] = bbb.get("DetailLedgerCode")
        cus_dict["Name"] = f'{bbb.get("FirstName")} {bbb.get("LastName")}'
    else:
        bbb = firms_coll.find_one({"PAMCode": trade_code}, {"_id": False})
        cus_dict["TradeCode"] = trade_code
        cus_dict["LedgerCode"] = bbb.get("DetailLedgerCode")
        cus_dict["Name"] = bbb.get("FirmTitle")

    ddd = commisions_coll.find_one(
        {
            "$and": [
                {"AccountCode": {"$regex": cus_dict["LedgerCode"]}},
                {"Date": {"$regex": gdate}},
            ]
        },
        {"_id": False},
    )
    if ddd:
        cus_dict["TBSBuyCo"] = ddd.get("NonOnlineBuyCommission") + ddd.get(
            "OnlineBuyCommission"
        )
        cus_dict["TBSSellCo"] = ddd.get("NonOnlineSellCommission") + ddd.get(
            "OnlineSellCommission"
        )
    else:
        cus_dict["TBSBuyCo"] = 0
        cus_dict["TBSSellCo"] = 0

    if aggre_dict:
        cus_dict["OurBuyCom"] = aggre_dict["items"][0]["TotalBuy"]
        cus_dict["OurSellCom"] = aggre_dict["items"][0]["TotalSell"]
    else:
        cus_dict["OurBuyCom"] = 0
        cus_dict["OurSellCom"] = 0

    cus_dict["BuyDiff"] = cus_dict["TBSBuyCo"] - cus_dict["OurBuyCom"]
    cus_dict["SellDiff"] = cus_dict["TBSSellCo"] - cus_dict["OurSellCom"]
    cus_dict["Date"] = date

    return cus_dict
