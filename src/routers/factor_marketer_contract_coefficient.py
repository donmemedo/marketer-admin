"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.schemas.factor_marketer_contract_coefficient import *
from src.tools.database import get_database

# from src.tools.tokens import JWTBearer, get_role_permission
from src.tools.utils import get_marketer_name, peek, to_gregorian_, check_permissions
from src.tools.logger import logger
from pymongo import MongoClient, errors
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize


marketer_contract_coefficient = APIRouter(prefix="/factor/marketer-contract-coefficient")


@marketer_contract_coefficient.post(
    "/add-marketer-contract-coefficient",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractCoefficient"],
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

    # database = get_database()
    coll = database["MarketerContractCoefficient"]
    marketers_coll = database["MarketerTable"]
    if mmcci.MarketerID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    filter = {"MarketerID": mmcci.MarketerID}
    update = {"$set": {}}
    if mmcci.ID is not None:
        update["$set"]["ID"] = mmcci.ID
    if mmcci.CoefficientPercentage is not None:
        update["$set"]["CoefficientPercentage"] = mmcci.CoefficientPercentage
    if mmcci.ContractID is not None:
        update["$set"]["ContractID"] = mmcci.ContractID
    if mmcci.HighThreshold is not None:
        update["$set"]["HighThreshold"] = mmcci.HighThreshold
    if mmcci.LowThreshold is not None:
        update["$set"]["LowThreshold"] = mmcci.LowThreshold
    if mmcci.StepNumber is not None:
        update["$set"]["StepNumber"] = mmcci.StepNumber
    if mmcci.Title is not None:
        update["$set"]["Title"] = mmcci.Title
    update["$set"]["CreateDateTime"] = str(datetime.now())
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    update["$set"]["IsCmdConcluded"] = False

    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpID": mmcci.MarketerID}, {"_id": False})
        )
        coll.insert_one({"MarketerID": mmcci.MarketerID, "Title": marketer_name})
        coll.update_one(filter, update)
    except:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP وارد شده در دیتابیس موجود است.", "code": "30003"},
        }
        return JSONResponse(status_code=409, content=resp)

        # coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmcci.MarketerID}, {"_id": False})
    return ResponseListOut(
        result=query_result,  # marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )



@marketer_contract_coefficient.put(
    "/modify-marketer-contract-coefficient",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractCoefficient"],
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

    # database = get_database()
    coll = database["MarketerContractCoefficient"]

    if mmcci.MarketerID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    filter = {"MarketerID": mmcci.MarketerID}
    update = {"$set": {}}
    if mmcci.ID is not None:
        update["$set"]["ID"] = mmcci.ID
    if mmcci.CoefficientPercentage is not None:
        update["$set"]["CoefficientPercentage"] = mmcci.CoefficientPercentage
    if mmcci.ContractID is not None:
        update["$set"]["ContractID"] = mmcci.ContractID
    if mmcci.HighThreshold is not None:
        update["$set"]["HighThreshold"] = mmcci.HighThreshold
    if mmcci.LowThreshold is not None:
        update["$set"]["LowThreshold"] = mmcci.LowThreshold
    if mmcci.StepNumber is not None:
        update["$set"]["StepNumber"] = mmcci.StepNumber
    if mmcci.Title is not None:
        update["$set"]["Title"] = mmcci.Title
    update["$set"]["IsCmdConcluded"] = False
    update["$set"]["UpdateDateTime"] = str(datetime.now())
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": mmcci.MarketerID}, {"_id": False})
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


@marketer_contract_coefficient.get(
    "/search-marketer-contract-coefficient",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractCoefficient"],
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
    args: SearchMarketerContractCoefficientIn = Depends(SearchMarketerContractCoefficientIn),
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

    # database = get_database()

    coll = database["MarketerContractCoefficient"]
    upa=[]
    if args.MarketerID:
        upa.append({"MarketerID":args.MarketerID})
    if args.ID:
        upa.append({"ID":args.ID})
    if args.CoefficientPercentage:
        upa.append({"CoefficientPercentage":{"$gte":args.CoefficientPercentage}})
    if args.HighThreshold:
        upa.append({"HighThreshold":{"$lte": args.HighThreshold}})
    if args.LowThreshold:
        upa.append({"LowThreshold":{"$gte": args.LowThreshold}})
    if args.ContractID:
        upa.append({"ContractID":{"$regex": args.ContractID}})
    if args.StepNumber:
        upa.append({"StepNumber":args.StepNumber})
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



@marketer_contract_coefficient.delete(
    "/delete-marketer-contract-coefficient",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractCoefficient"],
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
    args: DelMarketerMarketerContractCoefficientIn = Depends(DelMarketerMarketerContractCoefficientIn),
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

    # database = get_database()

    coll = database["MarketerContractCoefficient"]
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


@marketer_contract_coefficient.put(
    "/modify-marketer-contract-coefficient-status",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor - MarketerContractCoefficient"],
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

    # database = get_database()

    coll = database["MarketerContractCoefficient"]
    if dmcci.MarketerID is None:
        resp = {
            "result": [],
            "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        }
        return JSONResponse(status_code=412, content=resp)

    filter = {"MarketerID": dmcci.MarketerID}
    query_result = coll.find_one({"MarketerID": dmcci.MarketerID}, {"_id": False})
    status = query_result.get("IsCmdConcluded")
    update = {"$set": {}}
    update["$set"]["IsCmdConcluded"] = bool(status ^ 1)
    coll.update_one(filter, update)
    query_result = coll.find_one({"MarketerID": dmcci.MarketerID}, {"_id": False})
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

add_pagination(marketer_contract_coefficient)
