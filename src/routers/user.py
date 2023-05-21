"""_summary_

Returns:
    _type_: _description_
"""
from fastapi import APIRouter, Depends, Request, HTTPException
from src.tools.tokens import JWTBearer, get_sub
from src.tools.database import get_database
from src.tools.utils import to_gregorian_, peek
from datetime import datetime, timedelta
from khayyam import JalaliDatetime as jd
from src.schemas.user import (
    UserTradesIn,
    UserTradesOut,
    UsersListIn,
    ResponseOut,
    ResponseListOut,
)
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate


user = APIRouter(prefix="/user")


@user.get(
    "/user-trades",
    dependencies=[Depends(JWTBearer())],
    tags=["User"],
    # response_model=Page[UserTradesOut],
    response_model=None,
)
async def get_user_trades(request: Request, args: UserTradesIn = Depends(UserTradesIn)):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    database = get_database()
    results = []
    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")
    trades_coll = database["trades"]
    query = {
        "$and": [
            {"TradeCode": args.TradeCode},
            {"TradeDate": {"$gte": from_gregorian_date}},
            {"TradeDate": {"$lte": to_gregorian_date}},
        ]
    }
    query_result = trades_coll.find(query, {"_id": False})
    trades = dict(enumerate(query_result))
    for i in range(len(trades)):
        results.append(trades[i])

    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@user.get(
    "/users-list-by-volume",
    dependencies=[Depends(JWTBearer())],
    tags=["User"],
    response_model=None,
)
def users_list_by_volume(request: Request, args: UsersListIn = Depends(UsersListIn)):
    # get user id
    marketer_id = get_sub(request)

    db = get_database()

    customers_coll = db["customers"]
    firms_coll = db["firms"]

    trades_coll = db["trades"]
    marketers_coll = db["marketers"]

    # check if marketer exists and return his name
    query_result = marketers_coll.find({"IdpId": marketer_id})

    marketer_dict = peek(query_result)

    if marketer_dict.get("FirstName") == "":
        marketer_fullname = marketer_dict.get("LastName")
    elif marketer_dict.get("LastName") == "":
        marketer_fullname = marketer_dict.get("FirstName")
    else:
        marketer_fullname = (
            marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
        )

    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")
    query = {"$and": [{"Referer": marketer_fullname}]}
    if args.marketername:
        query = {"Referer": {"$regex": args.marketername}}

    # get all customers' TradeCodes
    # query = {"$and": [{"Referer": marketer_fullname}]}

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    firms_records = firms_coll.find(query, fields)
    trade_codes = [c.get("PAMCode") for c in customers_records] + [
        c.get("PAMCode") for c in firms_records
    ]
    #
    # pipeline = [
    #     {
    #         "$match": {
    #             "$and": [
    #                 {"TradeCode": {"$in": trade_codes}},
    #                 {"TradeDate": {"$gte": from_gregorian_date}},
    #                 {"TradeDate": {"$lte": to_gregorian_date}},
    #             ]
    #         }
    #     },
    #     {
    #         "$project": {
    #             "Price": 1,
    #             "Volume": 1,
    #             "Total": {"$multiply": ["$Price", "$Volume"]},
    #             "TotalCommission": 1,
    #             "TradeItemBroker": 1,
    #             "TradeCode": 1,
    #             "Commission": {
    #                 "$cond": {
    #                     "if": {"$eq": ["$TradeType", 1]},
    #                     "then": {
    #                         "$add": [
    #                             "$TotalCommission",
    #                             {"$multiply": ["$Price", "$Volume"]},
    #                         ]
    #                     },
    #                     "else": {
    #                         "$subtract": [
    #                             {"$multiply": ["$Price", "$Volume"]},
    #                             "$TotalCommission",
    #                         ]
    #                     },
    #                 }
    #             },
    #         }
    #     },
    #     {
    #         "$group": {
    #             "_id": "$TradeCode",
    #             "TotalFee": {"$sum": "$TradeItemBroker"},
    #             "TotalPureVolume": {"$sum": "$Commission"},
    #         }
    #     },
    #     {
    #         "$project": {
    #             "_id": 0,
    #             "TradeCode": "$_id",
    #             "TotalPureVolume": 1,
    #             "TotalFee": 1,
    #         }
    #     },
    #     {
    #         "$lookup": {
    #             "from": "customers",
    #             "localField": "TradeCode",
    #             "foreignField": "PAMCode",
    #             "as": "UserProfile",
    #         }
    #     },
    #     {"$unwind": "$UserProfile"},
    #     {
    #         "$project": {
    #             "TradeCode": 1,
    #             "TotalFee": 1,
    #             "TotalPureVolume": 1,
    #             "FirstName": "$UserProfile.FirstName",
    #             "LastName": "$UserProfile.LastName",
    #             "Username": "$UserProfile.Username",
    #             "Mobile": "$UserProfile.Mobile",
    #             "RegisterDate": "$UserProfile.RegisterDate",
    #             "BankAccountNumber": "$UserProfile.BankAccountNumber",
    #         }
    #     },
    #     {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
    #     {
    #         "$facet": {
    #             "metadata": [{"$count": "total"}],
    #             "items": [
    #                 {"$skip": (args.page - 1) * args.size},
    #                 {"$limit": args.size},
    #             ],
    #         }
    #     },
    #     {"$unwind": "$metadata"},
    #     {
    #         "$project": {
    #             "total": "$metadata.total",
    #             "items": 1,
    #         }
    #     },
    # ]

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

    if aggre_dict is None:
        return {}

    aggre_dict["page"] = args.page
    aggre_dict["size"] = args.size
    aggre_dict["pages"] = -(aggre_dict.get("totalCount") // -args.size)

    # return aggre_dict
    return ResponseOut(
        result=aggre_dict,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@user.get(
    "/users-total",
    dependencies=[Depends(JWTBearer())],
    tags=["User"],
    response_model=None,
)
def users_total(request: Request, args: UsersListIn = Depends(UsersListIn)):
    # get user id
    marketer_id = get_sub(request)
    db = get_database()

    customers_coll = db["customers"]
    trades_coll = db["trades"]
    marketers_coll = db["marketers"]

    # check if marketer exists and return his name
    query_result = marketers_coll.find({"IdpId": marketer_id})

    marketer_dict = peek(query_result)

    if marketer_dict.get("FirstName") == "":
        marketer_fullname = marketer_dict.get("LastName")
    elif marketer_dict.get("LastName") == "":
        marketer_fullname = marketer_dict.get("FirstName")
    else:
        marketer_fullname = (
            marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")
        )

    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
        days=1
    )
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    # get all customers' TradeCodes
    query = {"$and": [{"Referer": marketer_fullname}]}

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    trade_codes = [c.get("PAMCode") for c in customers_records]

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

    if aggre_dict is None:
        return {}

    aggre_dict["page"] = args.page
    aggre_dict["size"] = args.size
    aggre_dict["pages"] = -(aggre_dict.get("totalCount") // -args.size)

    return aggre_dict

@user.get(
    "/users-diff",
    dependencies=[Depends(JWTBearer())],
    tags=["Marketer"], response_model=None
)
async def users_diff_with_tbs(
        request: Request, args: UserTradesIn = Depends(UserTradesIn)
):

    # get user id
    # marketer_id = get_sub(request)
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
        print(date)

        q = bs_calculator(args.TradeCode, date)
        if q['BuyDiff'] == 0 and q['SellDiff'] == 0:
            pass
        else:
            result.append(q)
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(user)


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


# b=cost_calculator(['18691270430742','1866253420','1866257261','1866259792','1866246063','1866246295'],'1400-02-01','1402-02-21')
# print(b)
def bs_calculator(trade_code, date, page=1, size=10):
    db = get_database()
    trades_coll = db["trades"]
    customers_coll = db["customers"]
    firms_coll = db["firms"]

    commisions_coll = db["commisions"]
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
                "TotalSell": 1
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

    # if aggre_dict is None:
    #     return {}

    # aggre_dict["page"] = page
    # aggre_dict["size"] = size
    # aggre_dict["pages"] = -(aggre_dict.get("totalCount") // -size)
    customer = {}
    cus_dict = {}

    # for i in range(len(trade_codes)):

        # customer[i]=(cus_dict)
        # trade_code = trade_codes[i]
    bb = customers_coll.find_one({"PAMCode": trade_code}, {"_id": False})
    if bb:
        cus_dict["TradeCode"] = trade_code
        cus_dict["LedgerCode"] = bb.get("DetailLedgerCode")
        cus_dict["Name"] = f'{bb.get("FirstName")} {bb.get("LastName")}'
    else:
        bb = firms_coll.find_one({"PAMCode": trade_code}, {"_id": False})
        cus_dict["TradeCode"] = trade_code
        cus_dict["LedgerCode"] = bb.get("DetailLedgerCode")
        cus_dict["Name"] = bb.get("FirmTitle")


    dd = commisions_coll.find_one(
        {"$and": [{"AccountCode": {"$regex": cus_dict["LedgerCode"]}}, {"Date": {"$regex": gdate}}]},
        {"_id": False})
    if dd:
        cus_dict["TBSBuyCo"] = dd.get("NonOnlineBuyCommission") + dd.get("OnlineBuyCommission")
        cus_dict["TBSSellCo"] = dd.get("NonOnlineSellCommission") + dd.get("OnlineSellCommission")
    else:
        cus_dict["TBSBuyCo"] = 0
        cus_dict["TBSSellCo"] = 0

    if aggre_dict:
        cus_dict["OurBuyCom"] = aggre_dict['items'][0]["TotalBuy"]
        cus_dict["OurSellCom"] = aggre_dict['items'][0]["TotalSell"]
    else:
        cus_dict["OurBuyCom"] = 0
        cus_dict["OurSellCom"] = 0

    cus_dict["BuyDiff"] = cus_dict["TBSBuyCo"] - cus_dict["OurBuyCom"]
    cus_dict["SellDiff"] = cus_dict["TBSSellCo"] - cus_dict["OurSellCom"]
    cus_dict["Date"] = date

    # Comm = bs_calculator([trade_code], dato, dato)
        # print(Comm)
        # print(f"{BuyCo}\t{SellCo}\t{BuyCom}\t{SellCom}\t")
        # customer.append(cus_dict)


    return cus_dict#aggre_dict


