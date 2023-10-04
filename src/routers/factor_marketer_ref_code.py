"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.factor_marketer_ref_code import *
from src.tools.database import get_database
from src.tools.utils import get_marketer_name
from src.tools.queries import *
# marketer_ref_code = APIRouter(prefix="/factor/marketer-ref-code")
marketer_ref_code = APIRouter(prefix="/marketer-ref-code")


@marketer_ref_code.post(
    "/add",
    tags=["Factor - MarketerRefCode"],
)
@authorize(
    [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Write",
        "MarketerAdmin.Factor.Create",
        "MarketerAdmin.Factor.All",
    ]
)
async def add_marketer_ref_code(
    request: Request,
    mmrci: ModifyMarketerRefCodeIn,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (ModifyFactorIn, optional): _description_. Defaults to Depends(ModifyFactorIn).

    Raises:
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    coll = database["MarketerRefCode"]
    marketers_coll = database["MarketerTable"]
    if mmrci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": mmrci.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mmrci).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = str(datetime.now())
    update["$set"]["UpdateDateTime"] = str(datetime.now())

    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpID": mmrci.MarketerID}, {"_id": False})
        )
        coll.insert_one({"MarketerID": mmrci.MarketerID, "Title": marketer_name})
        coll.update_one(filter, update)
    except:
        raise RequestValidationError(TypeError, body={"code": "30007", "status": 409})
    query_result = coll.find_one({"MarketerID": mmrci.MarketerID}, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_ref_code.put(
    "/modify",
    tags=["Factor - MarketerRefCode"],
)
@authorize(
    [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Write",
        "MarketerAdmin.Factor.Update",
        "MarketerAdmin.Factor.All",
    ]
)
async def modify_marketer_ref_code(
    request: Request,
    mmrci: ModifyMarketerRefCodeIn,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (ModifyFactorIn, optional): _description_. Defaults to Depends(ModifyFactorIn).

    Raises:
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    coll = database["MarketerRefCode"]
    if mmrci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": mmrci.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mmrci).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmrci.MarketerID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_ref_code.get(
    "/search",
    tags=["Factor - MarketerRefCode"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def search_marketer_ref_code(
    request: Request,
    args: SearchMarketerRefCodeIn = Depends(SearchMarketerRefCodeIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (SearchFactorIn, optional): _description_. Defaults to Depends(SearchFactorIn).

    Raises:
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    coll = database["MarketerRefCode"]
    upa = []
    # for key, value in vars(args).items():
    #     if value is not None:
    #         upa.append({key: value})

    if args.MarketerID:
        upa.append({"MarketerID": args.MarketerID})
    if args.ID:
        upa.append({"ID": args.ID})
    if args.SubsidiaryCode:
        upa.append({"SubsidiaryCode": args.SubsidiaryCode})
    if args.BranchCode:
        upa.append({"BranchCode": args.BranchCode})
    if args.RefCode:
        upa.append({"RefCode": args.RefCode})
    if args.SubsidiaryTitle:
        upa.append({"SubsidiaryTitle": {"$regex": args.SubsidiaryTitle}})
    if args.BranchTitle:
        upa.append({"BranchTitle": {"$regex": args.BranchTitle}})
    if args.Type:
        upa.append({"Type": {"$regex": args.Type}})
    if args.Title:
        upa.append({"Title": {"$regex": args.Title}})
    if upa:
        query = {"$and": upa}
    else:
        query = {}

    query_result = (
        coll.find(query, {"_id": False})
        .skip(args.size * (args.page - 1))
        .limit(args.size)
    )
    total_count = coll.count_documents(query)
    marketers = dict(enumerate(query_result))
    results = []
    for i in range(len(marketers)):
        results.append(marketers[i])
    if not results:
        # raise RequestValidationError(TypeError, body={"code": "30008", "status": 404})
        return error_404(args.size, args.page, "30008")
    result = {}
    result["code"] = "Null"
    result["message"] = "Null"
    result["totalCount"] = total_count  # len(marketers)
    result["pagedData"] = results

    resp = {
        "result": result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(status_code=200, content=resp)


@marketer_ref_code.delete(
    "/delete",
    tags=["Factor - MarketerRefCode"],
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
    ]
)
async def delete_marketer_ref_code(
    request: Request,
    args: DelMarketerRefCodeIn = Depends(DelMarketerRefCodeIn),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (SearchFactorIn, optional): _description_. Defaults to Depends(SearchFactorIn).

    Raises:
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    user_id = role_perm["sub"]
    coll = database["MarketerRefCode"]
    if args.MarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 400})
    query_result = coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    result = [f"مورد مربوط به ماکتر {query_result.get('MarketerName')} پاک شد."]
    coll.delete_one({"MarketerID": args.MarketerID})
    resp = {
        "result": result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {"message": "Null", "code": "Null"},
    }
    return JSONResponse(status_code=200, content=resp)


add_pagination(marketer_ref_code)
