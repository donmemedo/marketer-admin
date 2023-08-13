"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.schemas.factor_marketer_ref_code import *
from src.tools.database import get_database
from fastapi.exceptions import RequestValidationError
from src.tools.utils import get_marketer_name, peek, to_gregorian_, check_permissions
from src.tools.logger import logger
from pymongo import MongoClient, errors
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize


marketer_ref_code = APIRouter(prefix="/factor/marketer-ref-code")


@marketer_ref_code.post(
    "/add-marketer_ref_code",
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
    permissions = [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Write",
        "MarketerAdmin.Factor.Create",
        "MarketerAdmin.Factor.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    coll = database["MarketerRefCode"]
    marketers_coll = database["MarketerTable"]
    if mmrci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": mmrci.MarketerID}
    update = {"$set": {}}
    if mmrci.ID is not None:
        update["$set"]["ID"] = mmrci.ID
    if mmrci.Title is not None:
        update["$set"]["Title"] = mmrci.Title
    if mmrci.Type is not None:
        update["$set"]["Type"] = mmrci.Type
    if mmrci.RefCode is not None:
        update["$set"]["RefCode"] = mmrci.RefCode
    if mmrci.SubsidiaryCode is not None:
        update["$set"]["SubsidiaryCode"] = mmrci.SubsidiaryCode
    if mmrci.SubsidiaryTitle is not None:
        update["$set"]["SubsidiaryTitle"] = mmrci.SubsidiaryTitle
    if mmrci.BranchCode is not None:
        update["$set"]["BranchCode"] = mmrci.BranchCode
    if mmrci.BranchTitle is not None:
        update["$set"]["BranchTitle"] = mmrci.BranchTitle
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
    "/modify-marketer-ref-code",
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
    permissions = [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Write",
        "MarketerAdmin.Factor.Update",
        "MarketerAdmin.Factor.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    coll = database["MarketerRefCode"]
    if mmrci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": mmrci.MarketerID}
    update = {"$set": {}}


    if mmrci.ID is not None:
        update["$set"]["ID"] = mmrci.ID
    if mmrci.Title is not None:
        update["$set"]["Title"] = mmrci.Title
    if mmrci.Type is not None:
        update["$set"]["Type"] = mmrci.Type
    if mmrci.RefCode is not None:
        update["$set"]["RefCode"] = mmrci.RefCode
    if mmrci.SubsidiaryCode is not None:
        update["$set"]["SubsidiaryCode"] = mmrci.SubsidiaryCode
    if mmrci.SubsidiaryTitle is not None:
        update["$set"]["SubsidiaryTitle"] = mmrci.SubsidiaryTitle
    if mmrci.BranchCode is not None:
        update["$set"]["BranchCode"] = mmrci.BranchCode
    if mmrci.BranchTitle is not None:
        update["$set"]["BranchTitle"] = mmrci.BranchTitle
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmrci.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_ref_code.get(
    "/search-marketer_ref_code",
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
    permissions = [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    coll = database["MarketerRefCode"]
    upa = []
    if args.MarketerID:
        upa.append({"MarketerID":args.MarketerID})
    if args.ID:
        upa.append({"ID":args.ID})
    if args.SubsidiaryCode:
        upa.append({"SubsidiaryCode":args.SubsidiaryCode})
    if args.SubsidiaryTitle:
        upa.append({"SubsidiaryTitle":{"$regex": args.SubsidiaryTitle}})
    if args.BranchCode:
        upa.append({"BranchCode":args.BranchCode})
    if args.BranchTitle:
        upa.append({"BranchTitle":{"$regex": args.BranchTitle}})
    if args.RefCode:
        upa.append({"RefCode":args.RefCode})
    if args.Type:
        upa.append({"Type":{"$regex": args.Type}})
    if args.Title:
        upa.append({"Title":{"$regex": args.Title}})
    query = {
        "$and": upa}

    query_result = coll.find(query, {"_id": False})
    marketers = dict(enumerate(query_result))
    results=[]
    for i in range(len(marketers)):
        results.append(marketers[i])
    if not results:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 204})
    result = {}
    result["code"] = "Null"
    result["message"] = "Null"
    result["totalCount"] = len(marketers)
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
    "/delete-marketer_ref_code",
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
    permissions = [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")
    coll = database["MarketerRefCode"]
    if args.MarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 400})
    query_result = coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
    result = [
        f"مورد مربوط به ماکتر {query_result.get('MarketerName')} پاک شد."
    ]
    coll.delete_one({"MarketerID": args.MarketerID})
    resp = {
        "result": result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {"message": "Null", "code": "Null"},
    }
    return JSONResponse(status_code=200, content=resp)


add_pagination(marketer_ref_code)
