from fastapi import APIRouter, Depends, Request, HTTPException
from src.tools.tokens import JWTBearer, get_sub
from src.tools.database import get_database
from src.tools.utils import to_gregorian_, peek
from datetime import datetime, timedelta
from src.schemas.user import UserTradesIn, UserTradesOut, UsersListIn
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate


user_router = APIRouter(prefix='/user')


@user_router.get("/user-trades", dependencies=[Depends(JWTBearer())], tags=["User"], response_model=Page[UserTradesOut])
async def get_marketer(request: Request, args: UserTradesIn = Depends(UserTradesIn)):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")
    
    database = get_database()

    trades_coll = database["trades"]


    return paginate(trades_coll, {})


@user_router.get("/users-total", dependencies=[Depends(JWTBearer())])
def users_list_by_volume(request: Request, args: UsersListIn = Depends(UsersListIn)):
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
        marketer_fullname = marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")


    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(days=1)
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    # get all customers' TradeCodes
    query = {"$and": [
        {"Referer":  marketer_fullname }
        ]
    }

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    trade_codes = [c.get('PAMCode') for c in customers_records]

    pipeline = [ 
        {
            "$match": {
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
                "Total" : {"$multiply": ["$Price", "$Volume"]},
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
        {
            "$lookup": {
                "from": "customers",
                "localField": "TradeCode",
                "foreignField": "PAMCode",
                "as": "UserProfile"
            }
        },
        {
            "$unwind": "$UserProfile"
        },
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
        {
            "$sort": {
                "TotalPureVolume": 1,
                "RegisterDate": 1,
                "TradeCode": 1 
            }
        },
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "items": [
                    {"$skip": (args.page - 1) * args.size }, 
                    {"$limit": args.size }
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

    if aggre_dict is None:
        return {}

    aggre_dict["page"] = args.page
    aggre_dict["size"] = args.size
    aggre_dict["pages"] = - ( aggre_dict.get("total") // - args.size )

    return aggre_dict


@user_router.get("/users-total", dependencies=[Depends(JWTBearer())], tags=["User"])
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
        marketer_fullname = marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")


    from_gregorian_date = to_gregorian_(args.from_date)
    to_gregorian_date = to_gregorian_(args.to_date)
    to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(days=1)
    to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")

    # get all customers' TradeCodes
    query = {"$and": [
        {"Referer":  marketer_fullname }
        ]
    }

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    trade_codes = [c.get('PAMCode') for c in customers_records]

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
                "Total" : {"$multiply": ["$Price", "$Volume"]},
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
        {
            "$lookup": {
                "from": "customers",
                "localField": "TradeCode",
                "foreignField": "PAMCode",
                "as": "UserProfile"
            }
        },
        {
            "$unwind": "$UserProfile"
        },
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
        {
            "$sort": {
                "TotalPureVolume": 1,
                "RegisterDate": 1,
                "TradeCode": 1 
            }
        },
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "items": [
                    {"$skip": (args.page - 1) * args.size }, 
                    {"$limit": args.size }
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

    if aggre_dict is None:
        return {}

    aggre_dict["page"] = args.page
    aggre_dict["size"] = args.size
    aggre_dict["pages"] = - ( aggre_dict.get("total") // - args.size )

    return aggre_dict


add_pagination(user_router)