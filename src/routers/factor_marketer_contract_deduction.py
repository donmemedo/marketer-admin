"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.schemas.factor_marketer_contract_deduction import *
from src.tools.database import get_database

# from src.tools.tokens import JWTBearer, get_role_permission
from src.tools.utils import get_marketer_name, peek, to_gregorian_, check_permissions
from src.tools.logger import logger
from pymongo import MongoClient, errors
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize


marketer_contract_deduction = APIRouter(prefix="/factor/marketer-contract-deduction")


@marketer_contract_deduction.post(
    "/add-marketer-contract-deduction",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractDeduction"],
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
async def add_marketer_contract_deduction(
    request: Request,
    mmcd: ModifyMarketerContractDeductionIn,
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
    # role_perm = get_role_permission(request)
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

    coll = database["MarketerContractDeduction"]
    marketers_coll = database["MarketerTable"]
    if mmcd.MarketerID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)

    filter = {"MarketerID": mmcd.MarketerID}
    update = {"$set": {}}
    if mmcd.ID is not None:
        update["$set"]["ID"] = mmcd.ID
    if mmcd.CollateralCoefficient is not None:
        update["$set"]["CollateralCoefficient"] = mmcd.CollateralCoefficient
    if mmcd.ContractID is not None:
        update["$set"]["ContractID"] = mmcd.ContractID
    if mmcd.TaxCoefficient is not None:
        update["$set"]["TaxCoefficient"] = mmcd.TaxCoefficient
    if mmcd.InsuranceCoefficient is not None:
        update["$set"]["InsuranceCoefficient"] = mmcd.InsuranceCoefficient
    if mmcd.ReturnDuration is not None:
        update["$set"]["ReturnDuration"] = mmcd.ReturnDuration
    if mmcd.Title is not None:
        update["$set"]["Title"] = mmcd.Title
    update["$set"]["CreateDateTime"] = str(datetime.now())
    update["$set"]["UpdateDateTime"] = str(datetime.now())

    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpID": mmcd.MarketerID}, {"_id": False})
        )
        coll.insert_one({"MarketerID": mmcd.MarketerID, "Title": marketer_name})
        coll.update_one(filter, update)
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP وارد شده در دیتابیس موجود است.", "code": "30003"},
        }
        return JSONResponse(status_code=409, content=resp)

        # coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmcd.MarketerID}, {"_id": False})
    return ResponseListOut(
        result=query_result,  # marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract_deduction.put(
    "/modify-marketer-contract-deduction",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractDeduction"],
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
async def modify_marketer_contract_deduction(
    request: Request,
    mmcd: ModifyMarketerContractDeductionIn,
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
    # role_perm = get_role_permission(request)
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

    coll = database["MarketerContractDeduction"]
    marketers_coll = database["MarketerTable"]
    if mmcd.MarketerID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)

    filter = {"MarketerID": mmcd.MarketerID}
    update = {"$set": {}}
    if mmcd.ID is not None:
        update["$set"]["ID"] = mmcd.ID
    if mmcd.CollateralCoefficient is not None:
        update["$set"]["CollateralCoefficient"] = mmcd.CollateralCoefficient
    if mmcd.ContractID is not None:
        update["$set"]["ContractID"] = mmcd.ContractID
    if mmcd.TaxCoefficient is not None:
        update["$set"]["TaxCoefficient"] = mmcd.TaxCoefficient
    if mmcd.InsuranceCoefficient is not None:
        update["$set"]["InsuranceCoefficient"] = mmcd.InsuranceCoefficient
    if mmcd.ReturnDuration is not None:
        update["$set"]["ReturnDuration"] = mmcd.ReturnDuration
    if mmcd.Title is not None:
        update["$set"]["Title"] = mmcd.Title
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmcd.MarketerID}, {"_id": False})
    if not query_result:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        }
        return JSONResponse(status_code=204, content=resp)

    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@marketer_contract_deduction.get(
    "/search-marketer-contract-deduction",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractDeduction"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def search_marketer_contract_deduction(
    request: Request,
    args: SearchMarketerContractDeductionIn = Depends(SearchMarketerContractDeductionIn),
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
    # role_perm = get_role_permission(request)
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
    coll = database["MarketerContractDeduction"]
    upa=[]
    if args.MarketerID:
        upa.append({"MarketerID":args.MarketerID})
    if args.ID:
        upa.append({"ID":args.ID})
    if args.ContractID:
        upa.append({"ContractID":{"$regex": args.ContractID}})
    if args.CollateralCoefficient:
        upa.append({"CollateralCoefficient":args.CollateralCoefficient})
    if args.TaxCoefficient:
        upa.append({"TaxCoefficient":args.TaxCoefficient})
    if args.InsuranceCoefficient:
        upa.append({"InsuranceCoefficient":args.InsuranceCoefficient})
    if args.ReturnDuration:
        upa.append({"StepNumber":args.ReturnDuration})
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
        result = {}
        result["code"] = "Null"
        result["message"] = "Null"
        # result["totalCount"] = len(marketers)
        # result["pagedData"] = results

        resp = {
            "result": result,
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "موردی برای متغیرهای داده شده یافت نشد.",
                "code": "30003",
            },
        }
        return JSONResponse(status_code=200, content=resp)
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


@marketer_contract_deduction.delete(
    "/delete-marketer-contract-deduction",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractDeduction"],
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
    ]
)
async def delete_marketer_contract_deduction(
    request: Request,
    args: DelMarketerContractDeductiontIn = Depends(DelMarketerContractDeductiontIn),
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
    # role_perm = get_role_permission(request)
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

    coll = database["MarketerContractDeduction"]
    if args.MarketerID:
        pass
    else:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {
                "message": "IDP مارکتر را وارد کنید.",
                "code": "30030",
            },
        }
        return JSONResponse(status_code=400, content=resp)
    query_result = coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})
    if not query_result:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        }
        return JSONResponse(status_code=204, content=resp)
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


add_pagination(marketer_contract_deduction)
