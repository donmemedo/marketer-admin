"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.schemas.factor import *
from src.tools.database import get_database
from fastapi.exceptions import RequestValidationError
# from src.tools.tokens import JWTBearer, get_role_permission
from src.tools.utils import get_marketer_name, peek, to_gregorian_, check_permissions
from src.tools.logger import logger
from pymongo import MongoClient, errors
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize


factor = APIRouter(prefix="/factor")


@factor.get(
    "/get-factor-consts",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def get_factors_consts(
    request: Request,
    args: MarketerIn = Depends(MarketerIn),
    brokerage: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_

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

    marketer_id = args.IdpID
    # brokerage = get_database()
    consts_coll = brokerage["consts"]
    query_result = consts_coll.find_one({"MarketerID": marketer_id}, {"_id": False})
    if not query_result:
        logger.error("No Record- Error 30001")
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "موردی یافت نشد.", "code": "30001"},
        # }
        # return JSONResponse(status_code=204, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی یافت نشد.", "code": "30001"},
        # )
    logger.info("Factor Constants were gotten Successfully.")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.get(
    "/get-all-factor-consts",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def get_all_factors_consts(
    request: Request,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_

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
    results = []
    consts_coll = database["consts"]
    query_result = consts_coll.find({}, {"_id": False})
    consts = dict(enumerate(query_result))
    for i in range(len(consts)):
        results.append((consts[i]))
    if not results:
        raise RequestValidationError(TypeError, body={"code": "30002", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "موردی برای ثابتهای فاکتورها یافت نشد.",
        #         "code": "30002",
        #     },
        # }
        # return JSONResponse(status_code=204, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "موردی برای ثابتهای فاکتورها یافت نشد.",
        #         "code": "30002",
        #     },
        # )
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.put(
    "/modify-factor-consts",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
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
async def modify_factor_consts(
    request: Request,
    mci: ModifyConstIn,
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (ModifyConstIn, optional): _description_. Defaults to Depends(ModifyConstIn).

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

    consts_coll = database["consts"]
    if mci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # }
        # return JSONResponse(status_code=412, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    filter = {"MarketerID": mci.MarketerID}
    update = {"$set": {}}

    if mci.FixIncome is not None:
        update["$set"]["FixIncome"] = mci.FixIncome

    if mci.Insurance is not None:
        update["$set"]["Insurance"] = mci.Insurance

    if mci.Collateral is not None:
        update["$set"]["Collateral"] = mci.Collateral

    if mci.Tax is not None:
        update["$set"]["Tax"] = mci.Tax

    consts_coll.update_one(filter, update)
    query_result = consts_coll.find_one({"MarketerID": mci.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "موردی با IDP داده شده یافت نشد.",
        #         "code": "30004",
        #     },
        # }
        # return JSONResponse(status_code=204, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "موردی با IDP داده شده یافت نشد.",
        #         "code": "30004",
        #     },
        # )
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.put(
    "/modify-factor",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
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
async def modify_factor(
    request: Request,
    mfi: ModifyFactorIn,
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

    factor_coll = database["factors"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # }
        # return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    filter = {"IdpID": mfi.MarketerID}
    update = {"$set": {}}
    per = mfi.Period

    if mfi.TotalPureVolume is not None:
        update["$set"][per + "TPV"] = mfi.TotalPureVolume

    if mfi.TotalFee is not None:
        update["$set"][per + "TF"] = mfi.TotalFee

    if mfi.PureFee is not None:
        update["$set"][per + "PureFee"] = mfi.PureFee

    if mfi.MarketerFee is not None:
        update["$set"][per + "MarFee"] = mfi.MarketerFee

    if mfi.Plan is not None:
        update["$set"][per + "Plan"] = mfi.Plan

    if mfi.Tax is not None:
        update["$set"][per + "Tax"] = mfi.Tax

    if mfi.Collateral is not None:
        update["$set"][per + "Collateral"] = mfi.Collateral

    if mfi.FinalFee is not None:
        update["$set"][per + "FinalFee"] = mfi.FinalFee

    if mfi.Payment is not None:
        update["$set"][per + "Payment"] = mfi.Payment

    if mfi.FactorStatus is not None:
        update["$set"][per + "FactStatus"] = mfi.FactorStatus

    factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"IdpID": mfi.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # }
        # return JSONResponse(status_code=204, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # )
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.post(
    "/add-factor",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
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
async def add_factor(
    request: Request,
    mfi: ModifyFactorIn,
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
    factor_coll = database["factors"]
    marketers_coll = database["marketers"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # }
        # return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "IDP مارکتر را وارد کنید.", "code": "30003"},
        # )

    filter = {"IdpID": mfi.MarketerID}
    update = {"$set": {}}
    per = mfi.Period

    if mfi.TotalPureVolume is not None:
        update["$set"][per + "TPV"] = mfi.TotalPureVolume

    if mfi.TotalFee is not None:
        update["$set"][per + "TF"] = mfi.TotalFee

    if mfi.PureFee is not None:
        update["$set"][per + "PureFee"] = mfi.PureFee

    if mfi.MarketerFee is not None:
        update["$set"][per + "MarFee"] = mfi.MarketerFee

    if mfi.Plan is not None:
        update["$set"][per + "Plan"] = mfi.Plan

    if mfi.Tax is not None:
        update["$set"][per + "Tax"] = mfi.Tax

    if mfi.Collateral is not None:
        update["$set"][per + "Collateral"] = mfi.Collateral

    if mfi.FinalFee is not None:
        update["$set"][per + "FinalFee"] = mfi.FinalFee

    if mfi.Payment is not None:
        update["$set"][per + "Payment"] = mfi.Payment

    if mfi.FactorStatus is not None:
        update["$set"][per + "FactStatus"] = mfi.FactorStatus

    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpID": mfi.MarketerID}, {"_id": False})
        )
        factor_coll.insert_one({"IdpID": mfi.MarketerID, "MarketerName": marketer_name})
        factor_coll.update_one(filter, update)
    except:
        factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"IdpID": mfi.MarketerID}, {"_id": False})
    return ResponseListOut(
        result=query_result,  # marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.get(
    "/search-factor",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
    ]
)
async def search_factor(
    request: Request,
    args: SearchFactorIn = Depends(SearchFactorIn),
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

    factor_coll = database["factors"]
    # if args.MarketerID and args.Period:
    if args.Period:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "IDP مارکتر و دوره را وارد کنید.",
        #         "code": "30030",
        #     },
        # }
        # return JSONResponse(status_code=400, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "IDP مارکتر و دوره را وارد کنید.",
        #         "code": "30030",
        #     },
        # )

    # filter = {"IdpID": args.MarketerID}
    per = args.Period
    if args.MarketerID:
        querry_result = factor_coll.find({"IdpID": args.MarketerID}, {"_id": False})
    else:
        querry_result = factor_coll.find({}, {"_id": False})
    if not querry_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # }
        # return JSONResponse(status_code=204, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # )

    results = []
    tma = per
    if int(per[4:6]) < 3:
        tma = str(int(per[0:4]) - 1) + str(int(per[4:6]) + 10)
    else:
        tma = str(int(per[0:4])) + f"{(int(per[4:6]) - 2):02}"

    qresult = dict(enumerate(querry_result))
    for i in range(len(qresult)):
        try:
            query_result = qresult[i]
            result = {}
            result["MarketerName"] = query_result.get("FullName")
            result["Doreh"] = per
            result["TotalPureVolume"] = query_result.get(per + "TPV")
            result["TotalFee"] = query_result.get(per + "TF")
            result["PureFee"] = query_result.get(per + "PureFee")
            result["MarketerFee"] = query_result.get(per + "MarFee")
            result["Plan"] = query_result.get(per + "Plan")
            result["Tax"] = query_result.get(per + "Tax")
            result["ThisMonthCollateral"] = query_result.get(per + "Collateral")
            result["TwoMonthsAgoCollateral"] = query_result.get(tma + "Collateral")
            result["FinalFee"] = query_result.get(per + "FinalFee")
            result["Payment"] = query_result.get(per + "Payment")
            result["FactStatus"] = query_result.get(per + "FactStatus")
            result["IdpID"] = query_result.get("IdpID")
            results.append(result)
            # query_result.values()
        except:
            raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
            # resp = {
            #     "result": [],
            #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     "error": {
            #         "message": "موردی در دیتابیس یافت نشد.",
            #         "code": "30001",
            #     },
            # }
            # return JSONResponse(status_code=204, content=resp)

            # return ResponseListOut(
            #     result=[],
            #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            #     error={
            #         "message": "موردی در دیتابیس یافت نشد.",
            #         "code": "30001",
            #     },
            # )
    if args.MarketerID:
        last_result = results
    else:
        last_result = {
            "code": "Null",
            "message": "Null",
            "totalCount": len(qresult),
            "pagedData": results,
        }
    return ResponseListOut(
        result=last_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.delete(
    "/delete-factor",
    # dependencies=[Depends(JWTBearer())],
    tags=["Factor"],
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
    ]
)
async def delete_factor(
    request: Request,
    args: SearchFactorIn = Depends(SearchFactorIn),
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

    factor_coll = database["factors"]
    if args.MarketerID and args.Period:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "IDP مارکتر و دوره را وارد کنید.",
        #         "code": "30030",
        #     },
        # }
        # return JSONResponse(status_code=400, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "IDP مارکتر و دوره را وارد کنید.",
        #         "code": "30030",
        #     },
        # )

    filter = {"IdpID": args.MarketerID}
    update = {"$set": {}}
    per = args.Period
    query_result = factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # }
        # return JSONResponse(status_code=204, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # )
    result = [
        f"از ماکتر {query_result.get('FullName')}فاکتور مربوط به دوره {args.Period} پاک شد."
    ]
    update = {"$unset": {}}
    update["$unset"][per + "PureFee"] = 1
    update["$unset"][per + "MarFee"] = 1
    update["$unset"][per + "TPV"] = 1
    update["$unset"][per + "TF"] = 1
    update["$unset"][per + "PureFee"] = 1
    update["$unset"][per + "MarFee"] = 1
    update["$unset"][per + "Plan"] = 1
    update["$unset"][per + "Tax"] = 1
    update["$unset"][per + "Collateral"] = 1
    update["$unset"][per + "FinalFee"] = 1
    update["$unset"][per + "Payment"] = 1
    update["$unset"][per + "FactStatus"] = 1
    try:
        factor_coll.update_one({"IdpID": args.MarketerID}, update)

    except:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 204})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # }
        # return JSONResponse(status_code=204, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={"message": "موردی در دیتابیس یافت نشد.", "code": "30001"},
        # )
    result.append(factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False}))
    return ResponseListOut(
        result=result,  # factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False}),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


add_pagination(factor)


# def cost_calculator(trade_codes, from_date, to_date, page=1, size=10):
#     """_summary_
#
#     Args:
#         trade_codes (_type_): _description_
#         from_date (_type_): _description_
#         to_date (_type_): _description_
#         page (int, optional): _description_. Defaults to 1.
#         size (int, optional): _description_. Defaults to 10.
#
#     Returns:
#         _type_: _description_
#     """
#     database = get_database()
#     trades_coll = database["trades"]
#     from_gregorian_date = to_gregorian_(from_date)
#     to_gregorian_date = to_gregorian_(to_date)
#     to_gregorian_date = datetime.strptime(to_gregorian_date, "%Y-%m-%d") + timedelta(
#         days=1
#     )
#     to_gregorian_date = to_gregorian_date.strftime("%Y-%m-%d")
#
#     pipeline = [
#         {
#             "$match": {
#                 "$and": [
#                     {"TradeCode": {"$in": trade_codes}},
#                     {"TradeDate": {"$gte": from_gregorian_date}},
#                     {"TradeDate": {"$lte": to_gregorian_date}},
#                 ]
#             }
#         },
#         {
#             "$project": {
#                 "Price": 1,
#                 "Volume": 1,
#                 "Total": {"$multiply": ["$Price", "$Volume"]},
#                 "TotalCommission": 1,
#                 "TradeItemBroker": 1,
#                 "TradeCode": 1,
#                 "Commission": {
#                     "$cond": {
#                         "if": {"$eq": ["$TradeType", 1]},
#                         "then": {
#                             "$add": [
#                                 "$TotalCommission",
#                                 {"$multiply": ["$Price", "$Volume"]},
#                             ]
#                         },
#                         "else": {
#                             "$subtract": [
#                                 {"$multiply": ["$Price", "$Volume"]},
#                                 "$TotalCommission",
#                             ]
#                         },
#                     }
#                 },
#             }
#         },
#         {
#             "$group": {
#                 "_id": "$TradeCode",
#                 "TotalFee": {"$sum": "$TradeItemBroker"},
#                 "TotalPureVolume": {"$sum": "$Commission"},
#             }
#         },
#         {
#             "$project": {
#                 "_id": 0,
#                 "TradeCode": "$_id",
#                 "TotalPureVolume": 1,
#                 "TotalFee": 1,
#             }
#         },
#         {
#             "$lookup": {
#                 "from": "firms",
#                 "localField": "TradeCode",
#                 "foreignField": "PAMCode",
#                 "as": "FirmProfile",
#             },
#         },
#         {"$unwind": {"path": "$FirmProfile", "preserveNullAndEmptyArrays": True}},
#         {
#             "$lookup": {
#                 "from": "customers",
#                 "localField": "TradeCode",
#                 "foreignField": "PAMCode",
#                 "as": "UserProfile",
#             }
#         },
#         {"$unwind": {"path": "$UserProfile", "preserveNullAndEmptyArrays": True}},
#         {
#             "$project": {
#                 "TradeCode": 1,
#                 "TotalFee": 1,
#                 "TotalPureVolume": 1,
#                 "Refferer": "$FirmProfile.Referer",
#                 "Referer": "$UserProfile.Referer",
#                 "FirmTitle": "$FirmProfile.FirmTitle",
#                 "FirmRegisterDate": "$FirmProfile.FirmRegisterDate",
#                 "FirmBankAccountNumber": "$FirmProfile.BankAccountNumber",
#                 "FirstName": "$UserProfile.FirstName",
#                 "LastName": "$UserProfile.LastName",
#                 "Username": "$UserProfile.Username",
#                 "Mobile": "$UserProfile.Mobile",
#                 "RegisterDate": "$UserProfile.RegisterDate",
#                 "BankAccountNumber": "$UserProfile.BankAccountNumber",
#             }
#         },
#         {"$sort": {"TotalPureVolume": 1, "RegisterDate": 1, "TradeCode": 1}},
#         {
#             "$facet": {
#                 "metadata": [{"$count": "totalCount"}],
#                 "items": [{"$skip": (page - 1) * size}, {"$limit": size}],
#             }
#         },
#         {"$unwind": "$metadata"},
#         {
#             "$project": {
#                 "totalCount": "$metadata.totalCount",
#                 "items": 1,
#             }
#         },
#     ]
#
#     aggr_result = trades_coll.aggregate(pipeline=pipeline)
#
#     aggre_dict = next(aggr_result, None)
#
#     if aggre_dict is None:
#         return {}
#
#     aggre_dict["page"] = page
#     aggre_dict["size"] = size
#     aggre_dict["pages"] = -(aggre_dict.get("totalCount") // -size)
#     return aggre_dict


# def totaliter(marketer_fullname, from_gregorian_date, to_gregorian_date):
#     """_summary_
#
#     Args:
#         marketer_fullname (_type_): _description_
#         from_gregorian_date (_type_): _description_
#         to_gregorian_date (_type_): _description_
#
#     Returns:
#         _type_: _description_
#     """
#     database = get_database()
#
#     customers_coll = database["customers"]
#     trades_coll = database["trades"]
#     firms_coll = database["firms"]
#     query = {"Referer": {"$regex": marketer_fullname}}
#     fields = {"PAMCode": 1}
#     customers_records = customers_coll.find(query, fields)
#     firms_records = firms_coll.find(query, fields)
#     trade_codes = [c.get("PAMCode") for c in customers_records] + [
#         c.get("PAMCode") for c in firms_records
#     ]
#
#     buy_pipeline = [
#         {
#             "$match": {
#                 "$and": [
#                     {"TradeCode": {"$in": trade_codes}},
#                     {"TradeDate": {"$gte": from_gregorian_date}},
#                     {"TradeDate": {"$lte": to_gregorian_date}},
#                     {"TradeType": 1},
#                 ]
#             }
#         },
#         {
#             "$project": {
#                 "Price": 1,
#                 "Volume": 1,
#                 "Total": {"$multiply": ["$Price", "$Volume"]},
#                 "TotalCommission": 1,
#                 "TradeItemBroker": 1,
#                 "Buy": {
#                     "$add": ["$TotalCommission", {"$multiply": ["$Price", "$Volume"]}]
#                 },
#             }
#         },
#         {
#             "$group": {
#                 "_id": "$id",
#                 "TotalFee": {"$sum": "$TradeItemBroker"},
#                 "TotalBuy": {"$sum": "$Buy"},
#             }
#         },
#         {"$project": {"_id": 0, "TotalBuy": 1, "TotalFee": 1}},
#     ]
#     sell_pipeline = [
#         {
#             "$match": {
#                 "$and": [
#                     {"TradeCode": {"$in": trade_codes}},
#                     {"TradeDate": {"$gte": from_gregorian_date}},
#                     {"TradeDate": {"$lte": to_gregorian_date}},
#                     {"TradeType": 2},
#                 ]
#             }
#         },
#         {
#             "$project": {
#                 "Price": 1,
#                 "Volume": 1,
#                 "Total": {"$multiply": ["$Price", "$Volume"]},
#                 "TotalCommission": 1,
#                 "TradeItemBroker": 1,
#                 "Sell": {
#                     "$subtract": [
#                         {"$multiply": ["$Price", "$Volume"]},
#                         "$TotalCommission",
#                     ]
#                 },
#             }
#         },
#         {
#             "$group": {
#                 "_id": "$id",
#                 "TotalFee": {"$sum": "$TradeItemBroker"},
#                 "TotalSell": {"$sum": "$Sell"},
#             }
#         },
#         {"$project": {"_id": 0, "TotalSell": 1, "TotalFee": 1}},
#     ]
#     buy_agg_result = peek(trades_coll.aggregate(pipeline=buy_pipeline))
#     sell_agg_result = peek(trades_coll.aggregate(pipeline=sell_pipeline))
#
#     buy_dict = {"vol": 0, "fee": 0}
#
#     sell_dict = {"vol": 0, "fee": 0}
#
#     if buy_agg_result:
#         buy_dict["vol"] = buy_agg_result.get("TotalBuy")
#         buy_dict["fee"] = buy_agg_result.get("TotalFee")
#
#     if sell_agg_result:
#         sell_dict["vol"] = sell_agg_result.get("TotalSell")
#         sell_dict["fee"] = sell_agg_result.get("TotalFee")
#     response_dict = {}
#     response_dict["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
#     response_dict["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
#     return response_dict
