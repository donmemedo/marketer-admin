"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.schemas.factor_marketer_contract import *
from src.tools.database import get_database
from fastapi.exceptions import RequestValidationError
from src.tools.utils import get_marketer_name, peek, to_gregorian_, check_permissions
from src.tools.logger import logger
from pymongo import MongoClient, errors
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize


# marketer_contract = APIRouter(prefix="/factor/marketer-contract")
marketer_contract = APIRouter(prefix="/marketer-contract")


@marketer_contract.post(
    "/add",
    tags=["Factor - MarketerContract"],
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
async def add_marketer_contract(
    request: Request,
    mmci: ModifyMarketerContractIn,
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
    coll = database["MarketerContract"]
    marketers_coll = database["MarketerTable"]
    if mmci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": mmci.MarketerID}
    update = {"$set": {}}
    update["$set"]["StartDate"] = jd.today().strftime("%Y-%m-%d")
    update["$set"]["EndDate"] = jd.today().replace(year=jd.today().year + 1).strftime("%Y-%m-%d")
    for key, value in vars(mmci).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = str(datetime.now())
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    update["$set"]["IsDeleted"] = False

    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpID": mmci.MarketerID}, {"_id": False})
        )
        coll.insert_one({"MarketerID": mmci.MarketerID, "Title": marketer_name})
        coll.update_one(filter, update)
    except:
        raise RequestValidationError(TypeError, body={"code": "30007", "status": 409})
    query_result = coll.find_one({"MarketerID": mmci.MarketerID}, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract.put(
    "/modify",
    tags=["Factor - MarketerContract"],
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
async def modify_marketer_contract(
    request: Request,
    mmci: ModifyMarketerContractIn,
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
    coll = database["MarketerContract"]
    if mmci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": mmci.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mmci).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    update["$set"]["IsDeleted"] = False
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmci.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract.get(
    "/search",
    tags=["Factor - MarketerContract"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def search_marketer_contract(
    request: Request,
    args: SearchMarketerContractIn = Depends(SearchMarketerContractIn),
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
    coll = database["MarketerContract"]
    upa=[]
    for key, value in vars(args).items():
        if value is not None:
            upa.append({key: value})
    if args.Description:
        upa.append({"Description":{"$regex": args.Description}})
    if args.EndDate:
        upa.append({"EndDate":{"$regex": args.EndDate}})
    if args.StartDate:
        upa.append({"StartDate":{"$regex": args.StartDate}})
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
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 200})
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


@marketer_contract.delete(
    "/delete",
    tags=["Factor - MarketerContract"],
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
    ]
)
async def delete_marketer_contract(
    request: Request,
    args: DelMarketerContractIn = Depends(DelMarketerContractIn),
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
    coll = database["MarketerContract"]
    if args.MarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 400})
    query_result = coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
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


@marketer_contract.put(
    "/modify-status",
    tags=["Factor - MarketerContract"],
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
async def modify_marketer_contract_status(
    request: Request,
    dmci: DelMarketerContractIn,
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
    coll = database["MarketerContract"]
    if dmci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"MarketerID": dmci.MarketerID}
    query_result = coll.find_one({"MarketerID": dmci.MarketerID}, {"_id": False})
    status = query_result.get("IsDeleted")
    update = {"$set": {}}
    update["$set"]["IsDeleted"] = bool(status ^ 1)
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": dmci.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(marketer_contract)
