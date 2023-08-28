"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from src.schemas.factors import *
from src.tools.database import get_database
from fastapi.exceptions import RequestValidationError
from src.tools.utils import get_marketer_name, peek, to_gregorian_, check_permissions
from src.tools.logger import logger
from src.tools.stages import plans
from src.tools.queries import *
from pymongo import MongoClient, errors
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from math import inf

factors = APIRouter(prefix="/new-factor")


@factors.post(
    "/base/add",
    tags=["Factors - Base"],
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
async def add_base_factor(
    request: Request,
    mfi: ModifyBaseFactorIn,
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
    factor_coll = database["MarketerFactor"]
    marketers_coll = database["MarketerTable"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    if mfi.ID is None:
        raise RequestValidationError(TypeError, body={"code": "30033", "status": 412})

    filter = {"MarketerID": mfi.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["CreateBy"] = user_id
    update["$set"]["UpdateBy"] = user_id
    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpId": mfi.MarketerID}, {"_id": False})
        )
        factor_coll.insert_one({"MarketerID": mfi.MarketerID, "Title": marketer_name})
        factor_coll.update_one(filter, update)
    except:
        factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"ID": mfi.ID}, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.put(
    "/base/modify",
    tags=["Factors - Base"],
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
async def modify_base_factor(
    request: Request,
    mfi: ModifyBaseFactorIn,
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
    factor_coll = database["MarketerFactor"]
    marketers_coll = database["MarketerTable"]
    if ((mfi.MarketerID and mfi.Period) or mfi.ID) is None:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 412})
    if mfi.ID:
        filter = {"ID": mfi.ID}
    else:
        filter = {"$and": [{"MarketerID": mfi.MarketerID}, {"Period": mfi.Period}]}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateBy"] = user_id

    try:
        factor_coll.update_one(filter, update)
    except:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 400})
    query_result = factor_coll.find_one({"IdpID": mfi.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 404})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.post(
    "/accounting/add",
    tags=["Factors - Accounting"],
)
@authorize(
    [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Accounting.Write",
        "MarketerAdmin.Accounting.Create",
        "MarketerAdmin.Accounting.All",
    ]
)
async def add_accounting_factor(
    request: Request,
    mfi: ModifyAccountingFactorIn,
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
    factor_coll = database["MarketerFactor"]
    marketers_coll = database["MarketerTable"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    if mfi.ID is None:
        raise RequestValidationError(TypeError, body={"code": "30033", "status": 412})

    filter = {"MarketerID": mfi.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["CreateBy"] = user_id
    update["$set"]["UpdateBy"] = user_id
    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpId": mfi.MarketerID}, {"_id": False})
        )
        factor_coll.insert_one({"MarketerID": mfi.MarketerID, "Title": marketer_name})
        factor_coll.update_one(filter, update)
    except:
        factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"ID": mfi.ID}, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.put(
    "/accounting/modify",
    tags=["Factors - Accounting"],
)
@authorize(
    [
        "MarketerAdmin.All.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Accounting.Write",
        "MarketerAdmin.Accounting.Update",
        "MarketerAdmin.Accounting.All",
    ]
)
async def modify_accounting_factor(
    request: Request,
    mfi: ModifyAccountingFactorIn,
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
    factor_coll = database["MarketerFactor"]
    marketers_coll = database["MarketerTable"]
    if ((mfi.MarketerID and mfi.Period) or mfi.ID) is None:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 412})
    if mfi.ID:
        filter = {"ID": mfi.ID}
    else:
        filter = {"$and": [{"MarketerID": mfi.MarketerID}, {"Period": mfi.Period}]}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateBy"] = user_id

    try:
        factor_coll.update_one(filter, update)
    except:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 400})
    query_result = factor_coll.find_one({"IdpID": mfi.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 404})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.get(
    "/search",
    tags=["Factors"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Read",
        "MarketerAdmin.Factor.All",
        "MarketerAdmin.Accounting.Read",
        "MarketerAdmin.Accounting.All",
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
    user_id = role_perm["sub"]
    factor_coll = database["MarketerFactor"]
    if args.Period:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})

    upa = []
    # for key, value in vars(args).items():
    #     if value is not None:
    #         upa.append({key: value})

    if args.MarketerID:
        upa.append({"MarketerID": args.MarketerID})
    if args.Period:
        upa.append({"Period": args.Period})
    if args.FactorStatus:
        upa.append({"FactorStatus": args.FactorStatus})
    if args.ID:
        upa.append({"ID": args.ID})
    if args.ContractID:
        upa.append({"ContractID": {"$regex": args.ContractID}})

    results = []
    filter = {"$and": upa}
    query_result = dict(enumerate(factor_coll.find(filter, {"_id": False})))
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})

    for i in range(len(query_result)):
        try:
            results.append(query_result[i])
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30001", "status": 200}
            )
    if args.MarketerID:
        last_result = results
    else:
        last_result = {
            "code": "Null",
            "message": "Null",
            "totalCount": len(query_result),
            "pagedData": results,
        }
    return ResponseListOut(
        result=last_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.delete(
    "/delete",
    tags=["Factors"],
)
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Factor.Delete",
        "MarketerAdmin.Factor.All",
        "MarketerAdmin.Accounting.Delete",
        "MarketerAdmin.Accounting.All",
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
    user_id = role_perm["sub"]
    factor_coll = database["MarketerFactor"]
    if args.ID:
        filter = {"ID": args.ID}
    elif args.ContractID and args.Period:
        filter = {
            "$and": [
                {"ContractID": args.ContractID},
                {"Period": args.Period},
            ]
        }
    elif args.MarketerID and args.Period:
        filter = {
            "$and": [
                {"MarketerID": args.MarketerID},
                {"Period": args.Period},
            ]
        }
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    query_result = factor_coll.find_one(filter, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    if args.ID:
        result = [f"فاکتور شماره  {args.ID} پاک شد."]
    else:

        result = [
            f"از ماکتر {query_result.get('Title')}فاکتور مربوط به دوره {args.Period} پاک شد."
        ]
    factor_coll.delete_one(filter)
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.get(
    "/calculate",
    tags=["Factors"],
)
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.MultiFactorCalculation.Read",
        "MarketerAdmin.MultiFactorCalculation.All",
    ]
)
async def calculate_factor(
    request: Request,
    args: CalFactorIn = Depends(CalFactorIn),
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
    factor_coll = database["newwfactors"]  # database["MarketerFactor"]
    marketer_coll = database["marketers"]#database["MarketerTable"]
    customer_coll = database["customers"]
    contract_coll = database["MarketerContract"]
    contded_coll = database["MarketerContractDeduction"]
    if args.Period:# and args.MarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    per = args.Period
    if args.MarketerID:
        marketers = marketer_coll.find_one({"IdpId": args.MarketerID}, {"_id": False})
    else:
        marketerrs = marketer_coll.find(
            {"IdpId": {"$exists": True, "$not": {"$size": 0}}}, {"_id": False}
        )
        marketers = dict(enumerate(marketerrs))
    results = []
    for num in marketers:
        marketer = marketers[num]

        # query = {"RefererTitle": marketer['Title']}
        # query = {"Referer": marketer["Title"]}
        query = {"Referer": get_marketer_name(marketer)}
        fields = {"TradeCodes": 1}


        customers_records = customer_coll.find(query, fields)
        # trade_codes = [c.get("TradeCodes") for c in customers_records]
        trade_codes = [c.get("PAMCode") for c in customers_records]
        gdate = jd.strptime(per, "%Y%m")
        from_gregorian_date = gdate.todatetime().isoformat()
        to_gregorian_date = (
            datetime.strptime(
                gdate.replace(day=gdate.daysinmonth).todate().isoformat(), "%Y-%m-%d"
            )
            + timedelta(days=1)
        ).isoformat()

        pipeline = [
            filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
            project_commission_stage(),
            group_by_total_stage("id"),
            project_pure_stage(),
        ]

        marketer_total = next(database.trades.aggregate(pipeline=pipeline), {"TotalPureVolume": 0, "TotalFee": 0})
        pure_fee = marketer_total.get("TotalFee") * 0.65
        marketer_fee = 0
        tpv = marketer_total.get("TotalPureVolume")
        b = plans
        try:
            cbt = contract_coll.find_one({"MarketerID": marketer['IdpId']}, {"_id": False})[
                "CalculationBaseType"
            ]
        except:
            cbt = ''
        for plan in plans[cbt]:
            plans[cbt][plan]["start"]
            if plans[cbt][plan]["start"] <= tpv < plans[cbt][plan]["end"]:
                marketer_fee = pure_fee * plans[cbt][plan]["marketer_share"]
                plan_name = plan
                if plans[cbt][plan]["end"] == inf:
                    next_plan = 0
                else:
                    next_plan = plans[cbt][plan]["end"] - tpv
        final_fee = marketer_fee
        try:
            salary = (
                contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})[
                    "Salary"
                ]
                * marketer_fee
            )
        except:
            salary = 0
        try:
            insurance = (
                contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})[
                    "InsuranceCoefficient"
                ]
                * marketer_fee
            )
        except:
            insurance = 0
        try:
            tax = (
                contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})[
                    "TaxCoefficient"
                ]
                * marketer_fee
            )
        except:
            tax = 0
        try:
            collateral = (
                contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})[
                    "CollateralCoefficient"
                ]
                * marketer_fee
            )
        except:
            collateral = 0
        deductions = salary + insurance + tax + collateral
        followers = dict(
            enumerate(
                database.mrelations.find({"LeaderMarketerID": args.MarketerID}, {"_id": 0})
            )
        )
        FTF = 0
        for i in followers:
            query = {"Referer": followers[i]["FollowerMarketerName"]}

            trade_codes = database.customers.distinct("PAMCode", query)
            from_gregorian_date = args.from_date
            to_gregorian_date = (
                datetime.strptime(args.to_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            pipeline = [
                filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
                project_commission_stage(),
                group_by_total_stage("id"),
                project_pure_stage(),
            ]
            fresult = next(database.trades.aggregate(pipeline=pipeline), [])
            FTF = FTF + fresult["TotalFee"] * followers[i]["CommissionCoefficient"]
        additions = FTF
        tmc = 0
        if args.Collateral:
            additions = additions + args.Collateral
            tmc = args.Collateral
        if args.Additions:
            additions = additions + args.Additions
        if args.Deductions:
            deductions = deductions + args.Deductions
        payment = final_fee + additions - deductions
        result = {
            "Title": get_marketer_name(marketer),#marketer["Title"],
            "MarketerID": marketer["IdpId"],#marketer["MarketerID"],
            "Period": args.Period,
            "TotalPureVolume": marketer_total.get("TotalPureVolume"),
            "TotalFee": marketer_total.get("TotalFee"),
            "PureFee": int(pure_fee),
            "MarketerFee": int(marketer_fee),
            "Plan": plan_name,
            "NextPlan": next_plan,
            "Tax": int(tax),
            "CollateralOfThisMonth": int(collateral),
            "SumOfPreviousCollaterals": tmc,
            "TotalFeeOfFollowers": FTF,
            "SumOfAdditions": int(additions),
            "SumOfDeductions": int(deductions),
            "FinalFee": int(final_fee),
            "Payment": int(payment),
        }
        try:
            factor_coll.insert_one(result)
            result.pop("_id")
            results.append(result)
        except:
            logger.critical(f"فاکتور {result['Title']} در دوره {result['Period']} موجود است.")
            result.pop("_id")
            results.append(result)
    resp = {
        "PagedData": results,
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(status_code=200, content=resp)
    # return ResponseOut(timeGenerated=datetime.now(), result=result, error="")


add_pagination(factors)
