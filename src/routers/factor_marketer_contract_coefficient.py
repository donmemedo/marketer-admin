"""_summary_

Returns:
    _type_: _description_
"""
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.factor_marketer_contract_coefficient import *
from src.tools.database import get_database
from src.tools.queries import *
from src.tools.utils import get_marketer_name

# marketer_contract_coefficient = APIRouter(prefix="/factor/marketer-contract-coefficient")
marketer_contract_coefficient = APIRouter(prefix="/marketer-contract-coefficient")


@marketer_contract_coefficient.post(
    "/add",
    tags=["MarketerContractCoefficient"],
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
async def add_marketer_contract_coefficient(
    request: Request,
    mmcci: AddMarketerContractCoefficientIn,
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
    coll = database["MarketerContractCoefficient"]
    marketers_coll = database["MarketerTable"]
    if mmcci.ContractID is None:
        raise RequestValidationError(TypeError, body={"code": "30034", "status": 412})
    filter = {"ContractID": mmcci.ContractID}
    update = {"$set": {}}
    for key, value in vars(mmcci).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = str(datetime.now())
    update["$set"]["ID"] = uuid.uuid1().hex
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    update["$set"]["IsCmdConcluded"] = False
    if mmcci.MarketerID:
        marketer = marketers_coll.find_one(
        {"MarketerID": mmcci.MarketerID}, {"_id": False}
        )
        if marketer:
            try:
                update["$set"]["Title"] = marketer["TbsReagentName"]
            except:
                update["$set"]["Title"] = marketer["Title"]
        else:
            # raise RequestValidationError(TypeError, body={"code": "30026", "status": 404})
            return error_404(0, 1, "30026")
    try:
        coll.insert_one(update["$set"])
    except:
        raise RequestValidationError(TypeError, body={"code": "30032", "status": 409})
    query_result = coll.find_one({"ContractID": mmcci.ContractID}, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract_coefficient.put(
    "/modify",
    tags=["MarketerContractCoefficient"],
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
async def modify_marketer_contract_coefficient(
    request: Request,
    mmcci: ModifyMarketerContractCoefficientIn,
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
    coll = database["MarketerContractCoefficient"]
    if mmcci.ContractID is None:
        raise RequestValidationError(TypeError, body={"code": "30034", "status": 412})
    filter = {"ContractID": mmcci.ContractID}
    update = {"$set": {}}
    for key, value in vars(mmcci).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["IsCmdConcluded"] = False
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    coll.update_one(filter, update)
    query_result = coll.find_one({"ContractID": mmcci.ContractID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract_coefficient.get(
    "/search",
    tags=["MarketerContractCoefficient"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def search_marketer_contract_coefficient(
    request: Request,
    args: SearchMarketerContractCoefficientIn = Depends(
        SearchMarketerContractCoefficientIn
    ),
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
    coll = database["MarketerContractCoefficient"]
    upa = []
    if args.MarketerID:
        upa.append({"MarketerID": args.MarketerID})
    if args.ID:
        upa.append({"ID": args.ID})
    if args.ContractID:
        upa.append({"ContractID": args.ContractID})
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
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(args.size, args.page, "30001")
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


@marketer_contract_coefficient.delete(
    "/delete",
    tags=["MarketerContractCoefficient"],
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
    ]
)
async def delete_marketer_contract_coefficient(
    request: Request,
    args: DelMarketerMarketerContractCoefficientIn = Depends(
        DelMarketerMarketerContractCoefficientIn
    ),
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
    coll = database["MarketerContractCoefficient"]
    if args.ContractID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30034", "status": 400})
    query_result = coll.find_one({"ContractID": args.ContractID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    result = [f"مورد مربوط به قرارداد {query_result.get('ContractID')} پاک شد."]
    coll.delete_one({"ContractID": args.ContractID})
    resp = {
        "result": result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {"message": "Null", "code": "Null"},
    }
    return JSONResponse(status_code=200, content=resp)


@marketer_contract_coefficient.put(
    "/modify-status",
    tags=["MarketerContractCoefficient"],
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
async def modify_marketer_contract_coefficient_status(
    request: Request,
    dmcci: DelMarketerMarketerContractCoefficientIn,
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
    coll = database["MarketerContractCoefficient"]
    if dmcci.ContractID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"ContractID": dmcci.ContractID}
    query_result = coll.find_one({"ContractID": dmcci.ContractID}, {"_id": False})
    if query_result:
        status = query_result.get("IsCmdConcluded")
        update = {"$set": {}}
        update["$set"]["IsCmdConcluded"] = bool(status ^ 1)
        coll.update_one(filter, update)
    query_result = coll.find_one({"ContractID": dmcci.ContractID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(marketer_contract_coefficient)
