"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi_pagination import add_pagination
from khayyam import JalaliDatetime as jd
from pymongo import MongoClient

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.schemas.factor import *
from src.tools.database import get_database
from src.tools.logger import logger
from src.tools.queries import *
from src.tools.stages import plans
from src.tools.utils import get_marketer_name, check_permissions

factor = APIRouter(prefix="/factor")


@factor.get(
    "/get-factor-consts",
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
    user_id = role_perm["sub"]
    marketer_id = args.IdpID
    consts_coll = brokerage["consts"]
    query_result = consts_coll.find_one({"MarketerID": marketer_id}, {"_id": False})
    if not query_result:
        logger.error("No Record- Error 30001")
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    logger.info("Factor Constants were gotten Successfully.")
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.get(
    "/get-all-factor-consts",
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
    user_id = role_perm["sub"]
    results = []
    consts_coll = database["consts"]
    query_result = consts_coll.find({}, {"_id": False})
    consts = dict(enumerate(query_result))
    for i in range(len(consts)):
        results.append((consts[i]))
    if not results:
        raise RequestValidationError(TypeError, body={"code": "30002", "status": 200})
    return ResponseListOut(
        result=results,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.put(
    "/modify-factor-consts",
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
    user_id = role_perm["sub"]
    consts_coll = database["consts"]
    if mci.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})

    filter = {"MarketerID": mci.MarketerID}
    update = {"$set": {}}

    for key, value in vars(mci).items():
        if value is not None:
            update["$set"][key] = value
    consts_coll.update_one(filter, update)
    query_result = consts_coll.find_one({"MarketerID": mci.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30004", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.put(
    "/modify-factor",
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
    mfi: ModifyFactorIN,
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
    factor_coll = database["factors"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})

    filter = {"IdpID": mfi.MarketerID}
    update = {"$set": {}}
    per = mfi.Period
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][per + key] = value
    update["$set"][f"{per}UpdateDateTime"] = datetime.now().isoformat()
    factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"IdpID": mfi.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.post(
    "/add-factor",
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
    mfi: ModifyFactorIN,
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
    factor_coll = database["factors"]
    marketers_coll = database["marketers"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})

    filter = {"IdpID": mfi.MarketerID}
    update = {"$set": {}}
    per = mfi.Period
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][per + key] = value
    update["$set"][f"{per}CreateDateTime"] = datetime.now().isoformat()
    update["$set"][f"{per}UpdateDateTime"] = datetime.now().isoformat()

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
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.get(
    "/search-factor",
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
    user_id = role_perm["sub"]
    factor_coll = database["factors"]
    if args.Period:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    per = args.Period
    if args.MarketerID:
        querry_result = factor_coll.find({"IdpID": args.MarketerID}, {"_id": False})
    else:
        querry_result = factor_coll.find({}, {"_id": False})
    if not querry_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
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
            # result["TotalPureVolume"] = query_result.get(per + "TPV")
            result["TotalPureVolume"] = query_result.get(per + "TotalPureVolume")
            result["TotalFee"] = query_result.get(per + "TotalFee")
            # result["TotalFee"] = query_result.get(per + "TF")
            result["PureFee"] = query_result.get(per + "PureFee")
            # result["MarketerFee"] = query_result.get(per + "MarFee")
            result["MarketerFee"] = query_result.get(per + "MarketerFee")
            result["Plan"] = query_result.get(per + "Plan")
            result["Tax"] = query_result.get(per + "SumOfDeductions")
            result["TotalFeeOfFollowers"] = query_result.get(
                per + "TotalFeeOfFollowers"
            )
            result["SumOfDeductions"] = query_result.get(per + "SumOfDeductions")
            # result["ThisMonthCollateral"] = query_result.get(per + "Collateral")
            result["ThisMonthCollateral"] = query_result.get(
                per + "CollateralOfThisMonth"
            )
            # result["TwoMonthsAgoCollateral"] = query_result.get(tma + "Collateral")
            result["TwoMonthsAgoCollateral"] = query_result.get(
                per + "TotalFeeOfFollowers"
            )
            result["FinalFee"] = query_result.get(per + "FinalFee")
            result["Payment"] = query_result.get(per + "Payment")
            result["FactStatus"] = query_result.get(per + "FactStatus")
            result["IdpID"] = query_result.get("IdpID")
            results.append(result)
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
    user_id = role_perm["sub"]
    factor_coll = database["factors"]
    if ((args.MarketerID or args.ContractID) and args.Period) or args.ID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    if args.ID:
        filter = {"IdpID": args.ID}

    filter = {"IdpID": args.MarketerID}
    update = {"$set": {}}
    per = args.Period
    update["$set"][f"{per}DeletedDateTime"] = datetime.now().isoformat()

    query_result = factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    try:
        factor_coll.update_one({"IdpID": args.MarketerID}, update)

    except:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    result = [
        f"از ماکتر {query_result.get('FullName')}فاکتور مربوط به دوره {args.Period} پاک شد."
    ]

    update = {"$unset": {}}
    update["$unset"][per + "TotalPureVolume"] = 1
    update["$unset"][per + "TotalFee"] = 1
    update["$unset"][per + "PureFee"] = 1
    update["$unset"][per + "MarketerFee"] = 1
    update["$unset"][per + "TotalFeeOfFollowers"] = 1
    update["$unset"][per + "CollateralOfThisMonth"] = 1
    update["$unset"][per + "SumOfDeductions"] = 1
    update["$unset"][per + "Payment"] = 1
    update["$unset"][per + "Collateral"] = 1
    update["$unset"][per + "FinalFee"] = 1
    update["$unset"][per + "Payment"] = 1
    update["$unset"][per + "FactorStatus"] = 1
    try:
        factor_coll.update_one({"IdpID": args.MarketerID}, update)

    except:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    result.append(factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False}))
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factor.get(
    "/calculate-factor",
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
    factor_coll = database["newfactors"]  # database["MarketerFactor"]
    marketer_coll = database["MarketerTable"]
    customer_coll = database["customersbackup"]
    contract_coll = database["MarketerContract"]
    contded_coll = database["MarketerContractDeduction"]
    factor_coll = database["newfactors"]  # database["MarketerFactor"]
    marketer_coll = database["marketers"]
    customer_coll = database["customers"]
    contract_coll = database["MarketerContract"]
    contded_coll = database["MarketerContractDeduction"]
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
        query = {"Referer": get_marketer_name(marketer)}
        fields = {"TradeCodes": 1}
        customers_records = customer_coll.find(query, fields)
        trade_codes = [c.get("TradeCodes") for c in customers_records]
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
        zimbo = {"TotalPureVolume": 0, "TotalFee": 0}
        marketer_total = next(database.trades.aggregate(pipeline=pipeline), zimbo)
        marketer_fee = 0
        b = plans
        marketer_plans = {
            "payeh": {"start": 0, "end": 30000000000, "marketer_share": 0.05},
            "morvarid": {
                "start": 30000000000,
                "end": 50000000000,
                "marketer_share": 0.1,
            },
            "firouzeh": {
                "start": 50000000000,
                "end": 100000000000,
                "marketer_share": 0.15,
            },
            "aghigh": {
                "start": 100000000000,
                "end": 150000000000,
                "marketer_share": 0.2,
            },
            "yaghout": {
                "start": 150000000000,
                "end": 200000000000,
                "marketer_share": 0.25,
            },
            "zomorod": {
                "start": 200000000000,
                "end": 300000000000,
                "marketer_share": 0.3,
            },
            "tala": {
                "start": 300000000000,
                "end": 400000000000,
                "marketer_share": 0.35,
            },
            "almas": {"start": 400000000000, "marketer_share": 0.4},
        }

        pure_fee = marketer_total.get("TotalFee") * 0.65
        # marketer_fee = 0
        tpv = marketer_total.get("TotalPureVolume")

        if marketer_plans["payeh"]["start"] <= tpv < marketer_plans["payeh"]["end"]:
            marketer_fee = pure_fee * marketer_plans["payeh"]["marketer_share"]
            plan = "Payeh"
            next_plan = marketer_plans["payeh"]["end"] - pure_fee
        elif (
            marketer_plans["morvarid"]["start"]
            <= tpv
            < marketer_plans["morvarid"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["morvarid"]["marketer_share"]
            plan = "Morvarid"
            next_plan = marketer_plans["morvarid"]["end"] - pure_fee
        elif (
            marketer_plans["firouzeh"]["start"]
            <= tpv
            < marketer_plans["firouzeh"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["firouzeh"]["marketer_share"]
            plan = "Firouzeh"
            next_plan = marketer_plans["firouzeh"]["end"] - pure_fee
        elif marketer_plans["aghigh"]["start"] <= tpv < marketer_plans["aghigh"]["end"]:
            marketer_fee = pure_fee * marketer_plans["aghigh"]["marketer_share"]
            plan = "Aghigh"
            next_plan = marketer_plans["aghigh"]["end"] - pure_fee
        elif (
            marketer_plans["yaghout"]["start"] <= tpv < marketer_plans["yaghout"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["yaghout"]["marketer_share"]
            plan = "Yaghout"
            next_plan = marketer_plans["yaghout"]["end"] - pure_fee
        elif (
            marketer_plans["zomorod"]["start"] <= tpv < marketer_plans["zomorod"]["end"]
        ):
            marketer_fee = pure_fee * marketer_plans["zomorod"]["marketer_share"]
            plan = "Zomorod"
            next_plan = marketer_plans["zomorod"]["end"] - pure_fee
        elif marketer_plans["tala"]["start"] <= tpv < marketer_plans["tala"]["end"]:
            marketer_fee = pure_fee * marketer_plans["tala"]["marketer_share"]
            plan = "Tala"
            next_plan = marketer_plans["tala"]["end"] - pure_fee
        elif marketer_plans["almas"]["start"] <= tpv:
            marketer_fee = pure_fee * marketer_plans["almas"]["marketer_share"]
            plan = "Almas"
            next_plan = 0
        final_fee = marketer_fee
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

        collateral = 0.1 * final_fee
        deductions = collateral  # salary + insurance + tax + collateral
        payment = final_fee - deductions + FTF
        result = {
            "IdpID": marketer["IdpId"],
            "FullName": get_marketer_name(marketer),
            "TotalPureVolume": marketer_total.get("TotalPureVolume"),
            "TotalFee": marketer_total.get("TotalFee"),
            "PureFee": int(pure_fee),
            "MarketerFee": int(marketer_fee),
            "Plan": plan,
            "NextPlan": next_plan,
            "TotalFeeOfFollowers": FTF,
            "CollateralOfThisMonth": int(collateral),
            "SumOfDeductions": int(deductions),
            "FinalFee": int(final_fee),
            "Payment": int(payment),
        }
        factor_coll.insert_one(result)
        result.pop("_id")
        results.append(result)
    resp = {"PagedData": results}
    return ResponseOut(timeGenerated=datetime.now(), result=resp, error="")


add_pagination(factor)
