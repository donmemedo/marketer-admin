"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.factor_marketer_contract import *
from src.tools.database import get_database
from src.tools.utils import get_marketer_name, check_permissions
from src.tools.stages import *
from src.tools.queries import *
import uuid

# marketer_contract = APIRouter(prefix="/factor/marketer-contract")
marketer_contract = APIRouter(prefix="/marketer-contract")


@marketer_contract.post("/add", tags=["MarketerContract"], response_model=None)
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
    mmci: AddMarketerContractIn,
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
    if marketers_coll.find_one(filter, {"_id": False}):
        pass
    else:
        # raise RequestValidationError(TypeError, body={"code": "30026", "status": 404})
        return error_404(0, 1, "30026")
    update = {"$set": {}}
    update["$set"]["StartDate"] = date.today().strftime("%Y-%m-%d")
    update["$set"]["EndDate"] = (
        date.today().replace(year=date.today().year + 1).strftime("%Y-%m-%d")
    )
    for key, value in vars(mmci).items():
        if value is not None:
            update["$set"][key] = value
    if mmci.StartDate or mmci.EndDate:
        try:
            StartDate = (
                jd(datetime.strptime(mmci.StartDate, "%Y-%m-%d")).date().isoformat()
            )
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30018", "status": 412}
            )
        try:
            EndDate = jd(datetime.strptime(mmci.EndDate, "%Y-%m-%d")).date().isoformat()
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30017", "status": 412}
            )

    update["$set"]["CreateDateTime"] = str(datetime.now())
    update["$set"]["ContractID"] = uuid.uuid1().hex
    update["$set"]["CoefficientBaseType"] = CoefficientBaseType[
        mmci.CoefficientBaseType.value
    ]
    update["$set"]["CalculationBaseType"] = CalculationBaseType[
        mmci.CalculationBaseType.value
    ]
    update["$set"]["ContractType"] = ContractType[mmci.ContractType.value]
    try:
        update["$set"]["Title"] = marketers_coll.find_one(
            {"MarketerID": mmci.MarketerID}, {"_id": False}
        )["TbsReagentName"]
    except:
        try:
            update["$set"]["Title"] = marketers_coll.find_one(
                {"MarketerID": mmci.MarketerID}, {"_id": False}
            )["Title"]
        except:
            update["$set"]["Title"] = "بی‌نام"

    update["$set"]["UpdateDateTime"] = str(datetime.now())
    update["$set"]["IsDeleted"] = False

    try:
        coll.insert_one(update["$set"])
    except:
        raise RequestValidationError(TypeError, body={"code": "30007", "status": 409})
    query_result = coll.find_one({"MarketerID": mmci.MarketerID}, {"_id": False})

    resp = {
        "result": query_result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(status_code=200, content=resp)


@marketer_contract.put(
    "/modify",
    tags=["MarketerContract"],
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
    if mmci.ContractID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"ContractID": mmci.ContractID}
    update = {"$set": {}}
    for key, value in vars(mmci).items():
        if value is not None:
            update["$set"][key] = value
    if mmci.StartDate or mmci.EndDate:
        try:
            StartDate = (
                jd(datetime.strptime(mmci.StartDate, "%Y-%m-%d")).date().isoformat()
            )
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30018", "status": 412}
            )
        try:
            EndDate = jd(datetime.strptime(mmci.EndDate, "%Y-%m-%d")).date().isoformat()
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30017", "status": 412}
            )

    update["$set"]["UpdateDateTime"] = str(datetime.now())
    update["$set"]["IsDeleted"] = False
    coll.update_one(filter, update)
    query_result = coll.find_one({"ContractID": mmci.ContractID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract.get(
    "/search",
    tags=["MarketerContract"],
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
    upa = []
    # for key, value in vars(args).items():
    #     if value is not None:
    #         upa.append({key: value})
    # ?CalculationBaseType: str = None
    # ?CoefficientBaseType: str = None
    if args.MarketerID:
        upa.append({"MarketerID": args.MarketerID})
    if args.ContractID:
        upa.append({"ContractID": args.ContractID})
    if args.ContractNumber:
        upa.append({"ContractNumber": args.ContractNumber})
    if args.ContractType:
        upa.append({"ContractType": ContractType[args.ContractType.value]})
    if args.CalculationBaseType:
        upa.append(
            {"CalculationBaseType": CalculationBaseType[args.CalculationBaseType.value]}
        )
    if args.CoefficientBaseType:
        upa.append(
            {"CoefficientBaseType": CoefficientBaseType[args.CoefficientBaseType.value]}
        )
    if args.Description:
        upa.append({"Description": {"$regex": args.Description}})
    if args.EndDate:
        upa.append({"EndDate": {"$lte": args.EndDate}})
    if args.StartDate:
        upa.append({"StartDate": {"$gte": args.StartDate}})
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


@marketer_contract.delete(
    "/delete",
    tags=["MarketerContract"],
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
    if args.ContractID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 400})
    query_result = coll.find_one({"ContractID": args.ContractID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    result = [f"مورد مربوط به ماکتر {query_result.get('Title')} پاک شد."]
    coll.delete_one({"ContractID": args.ContractID})
    resp = {
        "result": result,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {"message": "Null", "code": "Null"},
    }
    return JSONResponse(status_code=200, content=resp)


@marketer_contract.put(
    "/modify-status",
    tags=["MarketerContract"],
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
    if dmci.ContractID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    filter = {"ContractID": dmci.ContractID}
    query_result = coll.find_one({"ContractID": dmci.ContractID}, {"_id": False})
    if query_result:
        status = query_result.get("IsDeleted")
        update = {"$set": {}}
        update["$set"]["IsDeleted"] = bool(status ^ 1)
        coll.update_one(filter, update)
    query_result = coll.find_one({"ContractID": dmci.ContractID}, {"_id": False})
    if not query_result:
        # raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
        return error_404(0, 1, "30001")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(marketer_contract)
