"""_summary_

Returns:
    _type_: _description_
"""
import uuid
from datetime import datetime, timedelta
from math import inf

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.factors import *
from src.tools.database import get_database
from src.tools.logger import logger
from src.tools.queries import *
from src.tools.stages import plans
from src.tools.utils import get_marketer_name
from src.config import settings

factors = APIRouter(prefix="/factor")


# @factors.post(
#     "/base/add",
#     tags=["Factors - Base"],
# )
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
    factor_coll = database[settings.FACTOR_COLLECTION]  # ["MarketerFactor"]
    marketers_coll = database[settings.MARKETER_COLLECTION]  # ["MarketerTable"]
    if (mfi.MarketerID and mfi.Period) is None:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 412})
    # elif mfi.ID is None:
    #     raise RequestValidationError(TypeError, body={"code": "30033", "status": 412})
    filter = {"$and": [{"MarketerID": mfi.MarketerID}, {"Period": mfi.Period}]}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["FactorID"] = uuid.uuid1().hex
    update["$set"]["Status"] = 20

    update["$set"]["CreateBy"] = user_id
    update["$set"]["UpdateBy"] = user_id
    try:
        # marketer_name = get_marketer_name(
        #     marketers_coll.find_one({"MarketerID": mfi.MarketerID}, {"_id": False})
        # )
        marketer_name = marketers_coll.find_one(
            {"MarketerID": mfi.MarketerID}, {"_id": False}
        )["TbsReagentName"]

        factor_coll.insert_one(
            {"MarketerID": mfi.MarketerID, "Period": mfi.Period, "Title": marketer_name}
        )
        factor_coll.update_one(filter, update)
    except:
        factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one(
        {"FactorID": update["$set"]["FactorID"]}, {"_id": False}
    )
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.put(
    "/base",  # /modify",
    tags=["Factors"],  # - Base"],
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
    factor_coll = database[settings.FACTOR_COLLECTION]  # ["MarketerFactor"]
    marketers_coll = database[settings.MARKETER_COLLECTION]  # ["MarketerTable"]
    # if ((mfi.MarketerID and mfi.Period) or
    if mfi.FactorID is None:
        raise RequestValidationError(TypeError, body={"code": "30033", "status": 412})
    # if mfi.FactorID:
    filter = {"FactorID": mfi.FactorID}
    # else:
    #     filter = {"$and": [{"MarketerID": mfi.MarketerID}, {"Period": mfi.Period}]}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateBy"] = user_id
    # update["$set"]["Status"] = 20

    try:
        factor_coll.update_one(filter, update)
    except:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 400})
    query_result = factor_coll.find_one(filter, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 404})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


# @factors.post(
#     "/accounting/add",
#     tags=["Factors - Accounting"],
# )
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
    factor_coll = database[settings.FACTOR_COLLECTION]  # ["MarketerFactor"]
    marketers_coll = database[settings.MARKETER_COLLECTION]  # ["MarketerTable"]
    if ((mfi.MarketerID and mfi.Period) or mfi.FactorID) is None:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 412})
    if mfi.FactorID:
        filter = {"FactorID": mfi.FactorID}
    else:
        filter = {"$and": [{"MarketerID": mfi.MarketerID}, {"Period": mfi.Period}]}

    # if mfi.MarketerID is None:
    #     raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})
    # if mfi.ID is None:
    #     raise RequestValidationError(TypeError, body={"code": "30033", "status": 412})

    # filter = {"MarketerID": mfi.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    update["$set"]["CreateDateTime"] = jd.now().isoformat()
    update["$set"]["UpdateDateTime"] = jd.now().isoformat()
    update["$set"]["Status"] = 30
    update["$set"]["CreateBy"] = user_id
    update["$set"]["UpdateBy"] = user_id
    try:
        marketer_name = marketers_coll.find_one(
            {"MarketerID": mfi.MarketerID}, {"_id": False}
        )["TbsReagentName"]

        factor_coll.insert_one(
            {"MarketerID": mfi.MarketerID, "Period": mfi.Period, "Title": marketer_name}
        )
        factor_coll.update_one(filter, update)
    except:
        factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one(filter, {"_id": False})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.put(
    "/accounting",  # /modify",
    tags=["Factors"],  # - Accounting"],
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
    factor_coll = database[settings.FACTOR_COLLECTION]  # ["MarketerFactor"]
    marketers_coll = database[settings.MARKETER_COLLECTION]  # ["MarketerTable"]
    # if ((mfi.MarketerID and mfi.Period) or
    if mfi.FactorID is None:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 412})
    # if mfi.FactorID:
    filter = {"FactorID": mfi.FactorID}
    # else:
    #     filter = {"$and": [{"MarketerID": mfi.MarketerID}, {"Period": mfi.Period}]}
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
    query_result = factor_coll.find_one(filter, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30008", "status": 404})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


# @factors.get(
#     "/search",
#     tags=["Factors"],
# )
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
    factor_coll = database[settings.FACTOR_COLLECTION]  # ["MarketerFactor"]
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
    if args.FactorID:
        upa.append({"FactorID": args.FactorID})
    if args.ContractID:
        upa.append({"ContractID": {"$regex": args.ContractID}})

    results = []
    filter = {"$and": upa}
    query_result = dict(
        enumerate(
            factor_coll.find(filter, {"_id": False})
            .skip(args.size * (args.page - 1))
            .limit(args.size)
        )
    )
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})

    for i in range(len(query_result)):
        try:
            results.append(query_result[i])
        except:
            raise RequestValidationError(
                TypeError, body={"code": "30001", "status": 404}
            )
    if args.MarketerID:
        last_result = results
    else:
        last_result = {
            "code": "Null",
            "message": "Null",
            "totalCount": factor_coll.count_documents(filter),
            "pagedData": results,
        }
    return ResponseListOut(
        result=last_result,
        timeGenerated=jd.now().isoformat(),
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
    args: DeleteFactorIn = Depends(DeleteFactorIn),
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
    factor_coll = database[settings.FACTOR_COLLECTION]
    if args.FactorID:
        filter = {"FactorID": args.FactorID}
    # elif args.ContractID and args.Period:
    #     filter = {
    #         "$and": [
    #             {"ContractID": args.ContractID},
    #             {"Period": args.Period},
    #         ]
    #     }
    # elif args.MarketerID and args.Period:
    #     filter = {
    #         "$and": [
    #             {"MarketerID": args.MarketerID},
    #             {"Period": args.Period},
    #         ]
    #     }
    else:
        raise RequestValidationError(TypeError, body={"code": "30033", "status": 400})
    query_result = factor_coll.find_one(filter, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
    # if args.FactorID:
    #     result = [f"فاکتور شماره  {args.FactorID} پاک شد."]
    # else:
    result = [
        f"از مارکتر {query_result.get('Title')} فاکتور مربوط به دوره {query_result.get('Period')} پاک شد."
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
    factor_coll = database[settings.FACTOR_COLLECTION]  # database["MarketerFactor"]
    marketer_coll = database[settings.MARKETER_COLLECTION]  # database["MarketerTable"]
    customer_coll = database[settings.CUSTOMER_COLLECTION]
    contract_coll = database[settings.CONTRACT_COLLECTION]
    contded_coll = database[settings.CONTRACT_DEDUCTION_COLLECTION]
    if args.Period:  # and args.MarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    per = args.Period
    if len(per) != 6:
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
    if args.MarketerID:
        marketers = [
            marketer_coll.find_one(
                {"MarketerID": args.MarketerID},
                {"_id": False},
            )
        ]
        if marketers == [None]:
            raise RequestValidationError(TypeError, body={"code": "30026", "status": 404})
    else:
        marketerrs = marketer_coll.find(
            {"TbsReagentName": {"$exists": True, "$not": {"$size": 0}}},
            {"_id": False},
        )
        marketers = list(marketerrs)
    results = []
    for marketer in marketers:
        try:
            query = {"Referer": marketer["TbsReagentName"]}
        except:
            logger.error(f"مارکتر با شناسه {marketer['MarketerID']} معرف/بازاریاب نیست.")
            break
        fields = {"PAMCode": 1}

        customers_records = customer_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records]
        try:
            gdate = jd.strptime(per, "%Y%m")
        except:
            raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
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

        marketer_total = next(
            database.trades.aggregate(pipeline=pipeline),
            {"TotalPureVolume": 0, "TotalFee": 0},
        )
        pure_fee = marketer_total.get("TotalFee") * 0.65
        marketer_fee = 0
        tpv = marketer_total.get("TotalPureVolume")
        b = plans
        # ToDo:MarketerID must Change to ContractID because some Marketers may have multiple contracts
        contract_values = contract_coll.find_one(
            {"MarketerID": marketer["MarketerID"]}, {"_id": False}
        )
        if contract_values:
            try:
                cbt = contract_values["CalculationBaseType"]
            except:
                cbt = "نامشخص"
        else:
            raise RequestValidationError(TypeError, body={"code": "30031", "status": 404})
        for plan in plans[cbt]:
            # plans[cbt][plan]["start"]
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
                database.mrelations.find(
                    {"LeaderMarketerID": args.MarketerID}, {"_id": 0}
                )
            )
        )
        FTF = 0
        for i in followers:
            query = {"Referer": followers[i]["FollowerMarketerName"]}
            trade_codes = database.customers.distinct("PAMCode", query)
            pipeline = [
                filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
                project_commission_stage(),
                group_by_total_stage("id"),
                project_pure_stage(),
            ]
            fresult = next(
                database.trades.aggregate(pipeline=pipeline),
                {"TotalPureVolume": 0, "TotalFee": 0},
            )
            FTF = FTF + fresult["TotalFee"] * followers[i]["CommissionCoefficient"]
        additions = FTF
        # ToDo: TotalCMD AutoCalculation
        total_cmd = 0
        # if args.Additions:
        #     additions = additions + args.Additions
        # if args.Deductions:
        #     deductions = deductions + args.Deductions
        payment = final_fee + additions - deductions
        result = {
            "MarketerID": marketer["MarketerID"],  # marketer["MarketerID"],
            "Period": args.Period,
            "Title": marketer["TbsReagentName"],
            "TotalTurnOver": marketer_total.get("TotalPureVolume"),  # TotalPureVolume
            "TotalBrokerCommission": marketer_total.get("TotalFee"),
            "TotalNetBrokerCommission": int(pure_fee),
            "MarketerCommissionIncome": int(marketer_fee),
            # ToDo: Use uuid1 to audit MAC of Calculators and Timing for Security Reasons
            "FactorID": uuid.uuid1().hex,
            "TotalCMD": int(total_cmd),
            "Plan": plan_name,
            "NextPlanDiff": next_plan,
            "Tax": int(tax),
            "CollateralOfThisMonth": int(collateral),
            "TotalFeeOfFollowers": int(FTF),
            "SumOfAdditions": int(additions),
            "SumOfDeductions": int(deductions),
            "Status": 10,
            "Payment": int(payment),
            "IsCmdConcluded": False,
            "MaketerCMDIncome": 0,
            "TaxDeduction": 0,
            "TaxCoefficient": 0,
            "CollateralDeduction": 0,
            "CollateralCoefficient": 0,
            "InsuranceDeduction": 0,
            "InsuranceCoefficient": 0,
            "MarketerTotalIncome": 0,
            "CalculationCoefficient": 0,
            "ReturnDuration": 0,
            "InterimAmountDeduction": 0,
            "EmployeeSalaryDeduction": 0,
            "EmployerInsuranceDeduction": 0,
            "RedemptionDeduction": 0,
            "OtherDeduction": 0,
            "OtherDeductionDescription": " ",
            "CmdPayment": 0,
            "CollateralReturnPayment": 0,
            "InsuranceReturnPayment": 0,
            "OtherPayment": 0,
            "OtherPaymentDescription": " ",
            "CreateDateTime": jd.now().isoformat(),
            "UpdateDateTime": jd.now().isoformat(),
        }
        try:
            factor_coll.insert_one(result)
            result.pop("_id")
            results.append(result)
        except:
            logger.critical(
                f"فاکتور مربوط به {result['MarketerID']} در دوره {result['Period']} با شماره {result['FactorID']} موجود است."
            )
            result[
                "Error"
            ] = f"فاکتور مربوط به {result['MarketerID']} در دوره {result['Period']} با شماره {result['FactorID']} موجود است."
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


@factors.get("/get-all", tags=["Factors"], response_model=None)
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
async def get_marketer_all_factors(
    request: Request,
    role_perm: dict = Depends(get_role_permission),
    args: AllFactors = Depends(AllFactors),
    brokerage: MongoClient = Depends(get_database),
):
    factors_coll = brokerage[settings.FACTOR_COLLECTION]
    upa = []
    if args.MarketerID:
        upa.append({"MarketerID": args.MarketerID})
    if args.Period:
        upa.append({"Period": args.Period})
    if args.FactorStatus:
        upa.append({"FactorStatus": args.FactorStatus})
    if args.FactorID:
        upa.append({"FactorID": args.FactorID})

    results = []
    filter = {"$and": upa}
    if not (args.MarketerID or args.Period or args.FactorStatus or args.FactorID):
        filter = {}
    query_result = list(
        factors_coll.find(filter, {"_id": False})
        .skip(args.size * (args.page - 1))
        .limit(args.size)
    )
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 404})
    factors = sorted(
        query_result, key=lambda d: d["Period"], reverse=True
    )  # list(query_result)

    total_count = factors_coll.count_documents(filter)
    results = []
    for factor in factors:
        # results.append({factor.pop("FactorID"): factor})
        results.append(factor)

    resp = {
        "result": {
            "totalCount": total_count,
            "pagedData": results,
        },
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(status_code=200, content=resp)


add_pagination(factors)
