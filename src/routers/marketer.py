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
from src.schemas.marketer import *
from src.tools.database import get_database
from src.tools.queries import *
from src.tools.utils import *
from src.config import settings

marketer = APIRouter(prefix="/marketer")
marketer_relation = APIRouter(prefix="/marketer-relation")


# @marketer.post(
#     "/add",
#     tags=["Marketer"],
# )
@authorize(
    [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Create",
        "MarketerAdmin.Marketer.All",
    ]
)
async def add_marketer(
    request: Request,
    ami: AddMarketerIn,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (AddMarketerIn, optional): _description_. Defaults to Depends(AddMarketerIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    # admins_coll = database["admins"]
    marketer_coll = database[settings.MARKETER_COLLECTION]
    if ami.CurrentIdpId is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    # filter = {"IdpId": ami.CurrentIdpId}
    filter = {"Id": ami.CurrentIdpId}
    query_result = marketer_coll.find_one(filter, {"_id": False})
    if query_result:
        raise RequestValidationError(TypeError, body={"code": "30007", "status": 409})
    update = {"$set": {}}
    for key, value in vars(ami).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"][
        "CreateDate"
    ] = datetime.now().isoformat()  # jd.today().strftime("%Y-%m-%d")
    update["$set"][
        "ModifiedDate"
    ] = datetime.now().isoformat()  # jd.today().strftime("%Y-%m-%d")

    if ami.NationalID is not None:
        try:
            ddd = int(ami.NationalID)
            update["$set"]["Id"] = ami.NationalID
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30066", "status": 412}
            )
    # update["$set"]["IdpId"] = ami.CurrentIdpId
    update["$set"]["Id"] = ami.CurrentIdpId
    update["$set"].pop("CurrentIdpId")
    try:
        marketer_coll.insert_one(update["$set"])
    except:
        raise RequestValidationError(TypeError, body={"code": "30007", "status": 409})
    query_result = marketer_coll.find_one(filter, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30051", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


# @marketer.put(
#     "/modify",
#     tags=["Marketer"],
# )
@authorize(
    [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Write",
        "MarketerAdmin.Marketer.Update",
        "MarketerAdmin.Marketer.All",
    ]
)
async def modify_marketer(
    request: Request,
    mmi: ModifyMarketerIn,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (ModifyMarketerIn, optional): _description_. Defaults to Depends(ModifyMarketerIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    marketer_coll = database[settings.MARKETER_COLLECTION]
    admins_coll = database[settings.FACTOR_COLLECTION]
    if mmi.CurrentIdpId is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    # filter = {"IdpId": mmi.CurrentIdpId}
    filter = {"Id": mmi.CurrentIdpId}
    idpid = mmi.CurrentIdpId
    # query_result = marketer_coll.find_one({"IdpId": idpid}, {"_id": False})
    query_result = marketer_coll.find_one({"Id": idpid}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 400})
    update = {"$set": {}}
    for key, value in vars(mmi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"][
        "ModifiedDate"
    ] = datetime.now().isoformat()  # jd.today().strftime("%Y-%m-%d")

    if mmi.NewIdpId is not None:
        # update["$set"]["IdpId"] = mmi.NewIdpId
        update["$set"]["Id"] = mmi.NewIdpId
        idpid = mmi.NewIdpId

    if mmi.NationalID is not None:
        try:
            ddd = int(mmi.NationalID)
            update["$set"]["UniqueId"] = mmi.NationalID
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30066", "status": 412}
            )
    marketer_coll.update_one(filter, update)
    # query_result = marketer_coll.find_one({"IdpId": idpid}, {"_id": False})
    query_result = marketer_coll.find_one({"Id": idpid}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get(
    "/search",
    response_model=None,
    tags=["Marketer"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
)
async def search_user_profile(
    request: Request,
    args: MarketersProfileIn = Depends(MarketersProfileIn),
    brokerage: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (UserIn, optional): _description_. Defaults to Depends(UserIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    marketer_coll = brokerage[settings.MARKETER_COLLECTION]
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
    try:
        query_result = marketer_coll.find_one(query, {"_id": False})
    except:
        raise RequestValidationError(TypeError, body={"code": "30050", "status": 412})
    query_result = marketer_coll.find(query, {"_id": False})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketer_entity(marketers[i]))
    if not results:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 200})
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_relation.post(
    "/add",
    tags=["Marketer Relation"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Create",
        "MarketerAdmin.Marketer.All",
    ]
)
async def add_marketers_relations(
    request: Request,
    mrel: MarketerRelations,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (MarketerRelations, optional): _description_. Defaults to Depends(MarketerRelations).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    marketers_relations_coll = database[settings.RELATIONS_COLLECTION]
    marketers_coll = database[settings.MARKETER_COLLECTION]
    if mrel.LeaderMarketerID and mrel.FollowerMarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30009", "status": 412})
    try:
        d = float(mrel.CommissionCoefficient)
    except:
        raise RequestValidationError(TypeError, body={"code": "30010", "status": 412})
    update = {"$set": {}}

    update["$set"]["LeaderMarketerID"] = mrel.LeaderMarketerID
    update["$set"]["FollowerMarketerID"] = mrel.FollowerMarketerID
    if mrel.LeaderMarketerID == mrel.FollowerMarketerID:
        raise RequestValidationError(TypeError, body={"code": "30011", "status": 409})
    if marketers_relations_coll.find_one(
        {"FollowerMarketerID": mrel.FollowerMarketerID}
    ):
        if marketers_relations_coll.find_one(
            {"LeaderMarketerID": mrel.LeaderMarketerID}
        ):
            raise RequestValidationError(
                TypeError, body={"code": "30072", "status": 409}
            )
        else:
            raise RequestValidationError(
                TypeError, body={"code": "30012", "status": 406}
            )
    try:
        d = int(mrel.CommissionCoefficient)
        if 0 < mrel.CommissionCoefficient < 1:
            update["$set"]["CommissionCoefficient"] = mrel.CommissionCoefficient
        else:
            raise RequestValidationError(
                TypeError, body={"code": "30016", "status": 412}
            )
    except:
        raise RequestValidationError(TypeError, body={"code": "30015", "status": 412})
    update["$set"]["CreateDate"] = datetime.now().isoformat()
    update["$set"]["UpdateDate"] = update["$set"]["CreateDate"]
    update["$set"]["StartDate"] = datetime.today().date().isoformat()

    if mrel.StartDate is not None:
        update["$set"]["StartDate"] = mrel.StartDate
    if mrel.EndDate is not None:
        update["$set"]["EndDate"] = mrel.EndDate
        try:
            # update["$set"]["GEndDate"] = (
            (jd.strptime(update["$set"]["EndDate"], "%Y-%m-%d").todatetime())
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30017", "status": 412}
            )
    else:
        # update["$set"]["GEndDate"] = (
        jd.strptime("1500-12-29", "%Y-%m-%d").todatetime()
    try:
        # update["$set"]["GStartDate"] = (
        (jd.strptime(update["$set"]["StartDate"], "%Y-%m-%d").todatetime())
    except:
        raise RequestValidationError(TypeError, body={"code": "30018", "status": 412})
    if marketers_coll.find_one(
        # {"IdpId": mrel.FollowerMarketerID}
        {"Id": mrel.FollowerMarketerID}
    ) and marketers_coll.find_one(
        # {"IdpId": mrel.LeaderMarketerID}):
        {"Id": mrel.LeaderMarketerID}
    ):
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 400})
    # update["$set"]["FollowerMarketerName"] = get_marketer_name(
    #     marketers_coll.find_one({"IdpId": mrel.FollowerMarketerID})
    # )
    # update["$set"]["LeaderMarketerName"] = get_marketer_name(
    #     marketers_coll.find_one({"IdpId": mrel.LeaderMarketerID})
    # )
    update["$set"]["FollowerMarketerName"] = marketers_coll.find_one(
        {"Id": mrel.FollowerMarketerID}
    )["TbsReagentName"]
    update["$set"]["LeaderMarketerName"] = marketers_coll.find_one(
        {"Id": mrel.LeaderMarketerID}
    )["TbsReagentName"]

    marketers_relations_coll.insert_one(update["$set"])

    return ResponseListOut(
        result=marketers_relations_coll.find_one(
            {"FollowerMarketerID": mrel.FollowerMarketerID}, {"_id": False}
        ),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_relation.put(
    "/modify",
    tags=["Marketer Relation"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Write",
        "MarketerAdmin.Marketer.Update",
        "MarketerAdmin.Marketer.All",
    ]
)
async def modify_marketers_relations(
    request: Request,
    mrel: MarketerRelations,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (MarketerRelations, optional): _description_. Defaults to Depends(MarketerRelations).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    marketers_relations_coll = database[settings.RELATIONS_COLLECTION]
    if mrel.LeaderMarketerID and mrel.FollowerMarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30009", "status": 412})
    query = {
        "$and": [
            {"LeaderMarketerID": mrel.LeaderMarketerID},
            {"FollowerMarketerID": mrel.FollowerMarketerID},
        ]
    }
    if marketers_relations_coll.find_one(query) is None:
        raise RequestValidationError(TypeError, body={"code": "30019", "status": 200})
    if mrel.CommissionCoefficient is None:
        raise RequestValidationError(TypeError, body={"code": "30010", "status": 412})
    update = {"$set": {}}
    try:
        d = int(mrel.CommissionCoefficient)
        if 0 < mrel.CommissionCoefficient < 1:
            update["$set"]["CommissionCoefficient"] = mrel.CommissionCoefficient
        else:
            raise RequestValidationError(
                TypeError, body={"code": "30016", "status": 412}
            )
    except:
        raise RequestValidationError(TypeError, body={"code": "30015", "status": 412})
    update["$set"]["LeaderMarketerID"] = mrel.LeaderMarketerID
    update["$set"]["FollowerMarketerID"] = mrel.FollowerMarketerID
    if mrel.LeaderMarketerID == mrel.FollowerMarketerID:
        raise RequestValidationError(TypeError, body={"code": "30011", "status": 409})
    update["$set"]["CommissionCoefficient"] = mrel.CommissionCoefficient
    update["$set"]["UpdateDate"] = datetime.now().isoformat()
    update["$set"]["StartDate"] = datetime.today().date().isoformat()

    if mrel.StartDate is not None:
        update["$set"]["StartDate"] = mrel.StartDate
        try:
            # update["$set"]["GStartDate"] = (
            (jd.strptime(update["$set"]["StartDate"], "%Y-%m-%d").todatetime())
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30018", "status": 412}
            )
    if mrel.EndDate is not None:
        update["$set"]["EndDate"] = mrel.EndDate
        try:
            # update["$set"]["GEndDate"] = (
            (jd.strptime(update["$set"]["EndDate"], "%Y-%m-%d").todatetime())
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30017", "status": 412}
            )
        # if update["$set"]["GEndDate"] < update["$set"]["GStartDate"]:
        if update["$set"]["EndDate"] < update["$set"]["StartDate"]:
            raise RequestValidationError(
                TypeError, body={"code": "30071", "status": 400}
            )

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


@marketer_relation.get(
    "/search",
    tags=["Marketer Relation"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
)
async def search_marketers_relations(
    request: Request,
    args: SearchMarketerRelations = Depends(SearchMarketerRelations),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (SearchMarketerRelations, optional): _description_.
        Defaults to Depends(SearchMarketerRelations).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    try:
        StartDate = jd(datetime.strptime(args.StartDate, "%Y-%m-%d")).date().isoformat()
        from_gregorian_date = (
            jd.strptime(StartDate, "%Y-%m-%d").todatetime().isoformat()
        )
    except:
        raise RequestValidationError(TypeError, body={"code": "30018", "status": 412})
    try:
        EndDate = jd(datetime.strptime(args.EndDate, "%Y-%m-%d")).date().isoformat()
        to_gregorian_date = (
            jd.strptime(EndDate, "%Y-%m-%d").todatetime() + timedelta(days=1)
        ).isoformat()
    except:
        raise RequestValidationError(TypeError, body={"code": "30017", "status": 412})

    marketers_relations_coll = database[settings.RELATIONS_COLLECTION]
    upa = []
    query = {"$and": upa}

    if args.LeaderMarketerName:
        upa.append({"LeaderMarketerName": {"$regex": args.LeaderMarketerName}})
    if args.FollowerMarketerName:
        upa.append({"FollowerMarketerName": {"$regex": args.FollowerMarketerName}})
    if args.LeaderMarketerID:
        upa.append({"LeaderMarketerID": args.LeaderMarketerID})
    if args.FollowerMarketerID:
        upa.append({"FollowerMarketerID": args.FollowerMarketerID})
    upa.append({"StartDate": {"$gte": args.StartDate}})
    upa.append({"EndDate": {"$lte": args.EndDate}})

    results = []
    try:
        query_result = marketers_relations_coll.find_one(query, {"_id": False})
    except:
        raise RequestValidationError(TypeError, body={"code": "30050", "status": 412})
    query_result = marketers_relations_coll.find(query, {"_id": False})
    marketers = dict(enumerate(query_result))
    for i in range(len(marketers)):
        results.append(marketers[i])
    if not results:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 200})
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


@marketer_relation.delete(
    "/delete",
    tags=["Marketer Relation"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Delete",
        "MarketerAdmin.Marketer.All",
    ]
)
async def delete_marketers_relations(
    request: Request,
    args: DelMarketerRelations = Depends(DelMarketerRelations),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (DelMarketerRelations, optional): _description_.
            Defaults to Depends(DelMarketerRelations).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Delete",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")
    marketers_relations_coll = database[settings.RELATIONS_COLLECTION]
    marketers_coll = database[settings.MARKETER_COLLECTION]
    if args.LeaderMarketerID and args.FollowerMarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30009", "status": 400})
    if args.LeaderMarketerID == args.FollowerMarketerID:
        raise RequestValidationError(TypeError, body={"code": "30011", "status": 409})
    q = marketers_relations_coll.find_one(
        {"FollowerMarketerID": args.FollowerMarketerID}, {"_id": False}
    )

    if not q:
        raise RequestValidationError(TypeError, body={"code": "30052", "status": 200})
    if not q.get("LeaderMarketerID") == args.LeaderMarketerID:
        raise RequestValidationError(TypeError, body={"code": "30012", "status": 409})
    results = []
    # FollowerMarketerName = get_marketer_name(
    #     marketers_coll.find_one({"IdpId": args.FollowerMarketerID})
    # )
    # LeaderMarketerName = get_marketer_name(
    #     marketers_coll.find_one({"IdpId": args.LeaderMarketerID})
    # )
    FollowerMarketerName = marketers_coll.find_one({"IdpId": args.FollowerMarketerID})[
        "TbsReagentName"
    ]
    LeaderMarketerName = marketers_coll.find_one({"IdpId": args.LeaderMarketerID})[
        "TbsReagentName"
    ]

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


# @marketer.get(
#     "/users-diff-marketer",
#     tags=["Marketer"],
#     response_model=None,
# )
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
)
async def users_diff_with_tbs(
    request: Request,
    args: DiffTradesIn = Depends(DiffTradesIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (DiffTradesIn, optional): _description_. Defaults to Depends(DiffTradesIn).

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")
    customers_coll = database["customers"]
    # firms_coll = database["firms"]
    marketers_coll = database["marketers"]
    if args.IdpID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    query_result = marketers_coll.find({"IdpId": args.IdpID})
    marketer_dict = peek(query_result)
    marketer_fullname = get_marketer_name(marketer_dict)
    customers_records = customers_coll.find(
        {"Referer": marketer_fullname}, {"PAMCode": 1}
    )
    # firms_records = firms_coll.find({"Referer": marketer_fullname}, {"PAMCode": 1})
    trade_codes = [
        c.get("PAMCode") for c in customers_records
    ]  # + [c.get("PAMCode") for c in firms_records]
    try:
        to_date = jd(datetime.strptime(args.to_date, "%Y-%m-%d")).date().isoformat()
        from_date = jd(datetime.strptime(args.from_date, "%Y-%m-%d")).date().isoformat()
    except:
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
    start_date = jd.strptime(from_date, "%Y-%m-%d")
    end_date = jd.strptime(to_date, "%Y-%m-%d")

    delta = timedelta(days=1)
    dates = []
    while start_date < end_date:
        dates.append(str(start_date.date()))
        start_date += delta
    result = []
    for date in dates:
        for trade_code in trade_codes:
            q = bs_calculator(trade_code, date)
            if q["BuyDiff"] == 0 and q["SellDiff"] == 0:
                pass
            else:
                result.append(q)
    if not result:
        raise RequestValidationError(TypeError, body={"code": "30013", "status": 200})
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer.get("/all-users-total", tags=["Marketer"], response_model=None)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.All",
    ]
)
async def users_list_by_volume(
    role_perm: dict = Depends(get_role_permission),
    args: UsersListIn = Depends(UsersListIn),
    brokerage: MongoClient = Depends(get_database),
):
    if not args.IdpID:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    # query_result = brokerage.marketers.find_one({"IdpId": args.IdpID})
    marketer_col = brokerage[settings.MARKETER_COLLECTION]
    query_result = marketer_col.find_one({"Id": args.IdpID})

    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 200})
    # marketer_fullname = get_marketer_name(query_result)
    marketer_fullname = query_result["TbsReagentName"]

    from_gregorian_date = args.from_date
    to_gregorian_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    # query = {"Referer": {"$regex": marketer_fullname}}
    query = {"Referer": marketer_fullname}
    trade_codes = brokerage.customers.distinct("PAMCode", query)

    if args.user_type.value == "active":
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

        active_dict = next(brokerage.trades.aggregate(pipeline=pipeline), {})
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

    elif args.user_type.value == "inactive":
        active_users_pipeline = [
            filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
            group_by_trade_code_stage(),
            project_by_trade_code_stage(),
        ]

        active_users_res = brokerage.trades.aggregate(pipeline=active_users_pipeline)
        active_users_set = set(i.get("TradeCode") for i in active_users_res)

        # check whether it is empty or not
        inactive_users_set = set(trade_codes) - active_users_set

        inactive_users_pipeline = [
            match_inactive_users(list(inactive_users_set)),
            project_inactive_users(),
            sort_stage(args.sort_by.value, args.sort_order.value),
            paginate_data(args.page, args.size),
            unwind_metadata_stage(),
            project_total_stage(),
        ]

        inactive_dict = next(
            brokerage.customers.aggregate(pipeline=inactive_users_pipeline), {}
        )

        result = {}
        result["pagedData"] = inactive_dict.get("items", [])
        result["errorCode"] = None
        result["errorMessage"] = None
        result["totalCount"] = inactive_dict.get("total", 0)
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
    else:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "Null",
                "code": "Null",
            },
        }
        return JSONResponse(status_code=200, content=resp)


add_pagination(marketer)


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
        filter_users_stage(trade_code, from_gregorian_date, to_gregorian_date),
        project_commission_stage(),
        group_by_total_stage("$TradeCode"),
        project_pure_stage(),
        join_customers_stage(),
        unwind_user_stage(),
        project_fields_stage(),
        {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
        paginate_data(page, size),
        unwind_metadata_stage(),
        {"$project": {"totalCount": "$metadata.totalCount", "items": 1}},
    ]
    aggr_result = trades_coll.aggregate(pipeline=pipeline)
    aggre_dict = next(aggr_result, [])
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
