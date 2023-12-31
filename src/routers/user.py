"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.user import *
from src.tools.database import get_database
from src.tools.queries import *
from src.tools.utils import to_gregorian_, check_permissions

user = APIRouter(prefix="/user")


@user.get(
    "/user-trades",
    tags=["User"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
)
async def get_user_trades(
    request: Request,
    args: UserTradesIn = Depends(UserTradesIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (UserTradesIn, optional): _description_. Defaults to Depends(UserTradesIn).

    Raises:
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=401, detail="Not authorized.")

    if args.TradeCode is None:
        raise RequestValidationError(TypeError, body={"code": "30025", "status": 400})
    results = []
    from_gregorian_date = args.from_date
    to_gregorian_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

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

    if not results:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@user.get(
    "/users-list-by-volume",
    tags=["User"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
)
def users_list_by_volume(
    request: Request,
    args: UsersListIn = Depends(UsersListIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (UsersListIn, optional): _description_. Defaults to Depends(UsersListIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=401, detail="Not authorized.")
    customers_coll = database["customers"]
    # firms_coll = database["firms"]

    trades_coll = database["trades"]
    marketers_coll = database["marketers"]
    from_gregorian_date = args.from_date
    to_gregorian_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    query = {}  # {"$and": [{"Referer": ""}]}
    if args.marketername:
        query = {"Referer": {"$regex": args.marketername}}
    fields = {"PAMCode": 1}
    customers_records = customers_coll.find(query, fields)
    # firms_records = firms_coll.find(query, fields)
    trade_codes = [
        c.get("PAMCode") for c in customers_records
    ]  # + [c.get("PAMCode") for c in firms_records]
    pipeline = [
        filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
        project_commission_stage(),
        group_by_total_stage("$TradeCode"),
        project_pure_stage(),
        join_customers_stage(),
        unwind_user_stage(),
        project_fields_stage(),
        sort_stage(args.sort_by.value, args.sort_order.value),
        paginate_data(args.page, args.size),
        unwind_metadata_stage(),
        project_total_stage(),
    ]

    active_dict = next(database.trades.aggregate(pipeline=pipeline), {})
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


@user.get(
    "/users-total",
    tags=["User"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
)
def users_total(
    request: Request,
    args: UsersListIn = Depends(UsersListIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (UsersListIn, optional): _description_. Defaults to Depends(UsersListIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=401, detail="Not authorized.")
    customers_coll = database["customers"]
    # firms_coll = database["firms"]
    trades_coll = database["trades"]
    marketers_coll = database["marketers"]
    from_gregorian_date = args.from_date
    to_gregorian_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    query = {}  # {"$and": [{"Referer": ""}]}

    fields = {"PAMCode": 1}

    customers_records = customers_coll.find(query, fields)
    # firms_records = firms_coll.find(query, fields)
    trade_codes = [
        c.get("PAMCode") for c in customers_records
    ]  # + [c.get("PAMCode") for c in firms_records]

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
    if not aggre_dict:
        raise RequestValidationError(TypeError, body={"code": "30028", "status": 404})
    return ResponseOut(
        result=aggre_dict,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@user.get(
    "/users-diff",
    tags=["User"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
)
async def users_diff_with_tbs(
    request: Request,
    args: UserTradesIn = Depends(UserTradesIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (UserTradesIn, optional): _description_. Defaults to Depends(UserTradesIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.User.Read",
        "MarketerAdmin.User.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=401, detail="Not authorized.")
    try:
        to_date = jd(datetime.strptime(args.to_date, "%Y-%m-%d")).date().isoformat()
        from_date = jd(datetime.strptime(args.from_date, "%Y-%m-%d")).date().isoformat()
    except:
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})

    start_date = jd.strptime(from_date, "%Y-%m-%d")
    end_date = jd.strptime(to_date, "%Y-%m-%d")
    if args.TradeCode is None:
        raise RequestValidationError(TypeError, body={"code": "30025", "status": 400})
    delta = timedelta(days=1)
    dates = []
    while start_date < end_date:
        dates.append(str(start_date.date()))
        start_date += delta
    result = []
    for date in dates:
        print(date)

        q = bs_calculator(args.TradeCode, date)
        if q["BuyDiff"] == 0 and q["SellDiff"] == 0:
            pass
        else:
            result.append(q)
    if not result:
        raise RequestValidationError(TypeError, body={"code": "30013", "status": 404})
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(user)


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
