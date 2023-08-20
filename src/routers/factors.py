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
    "/add-factor",
    tags=["Factors"],
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
        result=query_result,  # marketer_entity(marketer_dict),
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.put(
    "/modify-factor",
    tags=["Factors"],
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

    factor_coll = database["MarketerFactor"]
    marketers_coll = database["MarketerTable"]
    if ((mfi.MarketerID and mfi.Period) or mfi.ID) is None:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 412})
    if mfi.ID:
        filter = {"ID": mfi.ID}
    # else:
    #     filter = {
    #         "$and": [
    #             {"TradeCode": {"$in": trade_codes}},
    #             {"TradeDate": {"$gte": from_gregorian_date}},
    #             {"TradeDate": {"$lte": to_gregorian_date}},
    #         ]
    #     }
    #     {"MarketerID": mfi.MarketerID}
    update = {"$set": {}}
    for key, value in vars(mfi).items():
        if value is not None:
            update["$set"][key] = value
    try:
        marketer_name = get_marketer_name(
            marketers_coll.find_one({"IdpId": mfi.MarketerID}, {"_id": False})
        )
        factor_coll.insert_one({"MarketerID": mfi.MarketerID, "Title": marketer_name})
        factor_coll.update_one(filter, update)
    except:
        factor_coll.update_one(filter, update)
    query_result = factor_coll.find_one({"ID": mfi.ID}, {"_id": False})

    factor_coll = database["factors"]
    if mfi.MarketerID is None:
        raise RequestValidationError(TypeError, body={"code": "30003", "status": 412})

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
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    return ResponseListOut(
        result=query_result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.get(
    "/search-factor",
    tags=["Factors"],
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
        except:
            raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
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


@factors.delete(
    "/delete-factor",
    tags=["Factors"],
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
    factor_coll = database["MarketerFactor"]
    # if ((args.MarketerID or args.ContractID) and args.Period) or args.ID:
    #     pass
    # else:
    #     raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    if args.ID:
        filter = {"ID": args.ID}
    elif args.ContractID and args.Period:
        filter = {"$and": [
            {"ContractID": args.ContractID},
            {"Period": args.Period},
        ]
        }
    elif args.MarketerID and args.Period:
        filter = {"$and": [
                    {"MarketerID": args.MarketerID},
                    {"Period": args.Period},
                ]
            }
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    # filter = {"IdpID": args.MarketerID}
    update = {"$set": {}}
    per = args.Period
    query_result = factor_coll.find_one(filter, {"_id": False})
    if not query_result:
        raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    if args.ID:
        result = [
            f"فاکتور شماره  {args.ID} پاک شد."
        ]
    else:

        result = [
            f"از ماکتر {query_result.get('Title')}فاکتور مربوط به دوره {args.Period} پاک شد."
        ]
    factor_coll.delete_one(filter)
    # update = {"$unset": {}}
    # update["$unset"][per + "PureFee"] = 1
    # update["$unset"][per + "MarFee"] = 1
    # update["$unset"][per + "TPV"] = 1
    # update["$unset"][per + "TF"] = 1
    # update["$unset"][per + "PureFee"] = 1
    # update["$unset"][per + "MarFee"] = 1
    # update["$unset"][per + "Plan"] = 1
    # update["$unset"][per + "Tax"] = 1
    # update["$unset"][per + "Collateral"] = 1
    # update["$unset"][per + "FinalFee"] = 1
    # update["$unset"][per + "Payment"] = 1
    # update["$unset"][per + "FactStatus"] = 1
    # try:
    #     factor_coll.update_one({"IdpID": args.MarketerID}, update)
    #
    # except:
    #     raise RequestValidationError(TypeError, body={"code": "30001", "status": 200})
    # result.append(factor_coll.find_one({"IdpID": args.MarketerID}, {"_id": False}))
    return ResponseListOut(
        result=result,
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@factors.get(
    "/calculate-factor",
    tags=["Factors"],
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
    factor_coll = database["newfactors"]#database["MarketerFactor"]
    marketer_coll = database["MarketerTable"]
    customer_coll = database["customersbackup"]
    contract_coll = database["MarketerContract"]
    contded_coll = database["MarketerContractDeduction"]
    if args.Period and args.MarketerID:
        pass
    else:
        raise RequestValidationError(TypeError, body={"code": "30030", "status": 400})
    per = args.Period
    marketer = marketer_coll.find_one({"IdpId": args.MarketerID}, {"_id": False})
    query = {"RefererTitle": marketer['Title']}
    fields = {"TradeCodes": 1}
    customers_records = customer_coll.find(query, fields)
    trade_codes = [c.get("TradeCodes") for c in customers_records]
    gdate = jd.strptime(per,"%Y%m")
    from_gregorian_date = gdate.todatetime().isoformat()
    to_gregorian_date = (datetime.strptime(gdate.replace(day=gdate.daysinmonth).todate().isoformat(),"%Y-%m-%d")+timedelta(days=1)).isoformat()

    pipeline = [
        filter_users_stage(trade_codes, from_gregorian_date, to_gregorian_date),
        project_commission_stage(),
        group_by_total_stage("id"),
        project_pure_stage()
    ]

    marketer_total = next(database.trades.aggregate(pipeline=pipeline), [])
    #
    # buy_pipeline = [
    #     {
    #         "$match": {
    #             "$and": [
    #                 {"TradeCode": {"$in": trade_codes}},
    #                 {"TradeDate": {"$gte": from_gregorian_date}},
    #                 {"TradeDate": {"$lte": to_gregorian_date}},
    #                 {"TradeType": 1},
    #             ]
    #         }
    #     },
    #     {
    #         "$project": {
    #             "Price": 1,
    #             "Volume": 1,
    #             "Total": {"$multiply": ["$Price", "$Volume"]},
    #             "TotalCommission": 1,
    #             "TradeItemBroker": 1,
    #             "Buy": {
    #                 "$add": ["$TotalCommission", {"$multiply": ["$Price", "$Volume"]}]
    #             },
    #         }
    #     },
    #     {
    #         "$group": {
    #             "_id": "$id",
    #             "TotalFee": {"$sum": "$TradeItemBroker"},
    #             "TotalBuy": {"$sum": "$Buy"},
    #         }
    #     },
    #     {"$project": {"_id": 0, "TotalBuy": 1, "TotalFee": 1}},
    # ]
    #
    # sell_pipeline = [
    #     {
    #         "$match": {
    #             "$and": [
    #                 {"TradeCode": {"$in": trade_codes}},
    #                 {"TradeDate": {"$gte": from_gregorian_date}},
    #                 {"TradeDate": {"$lte": to_gregorian_date}},
    #                 {"TradeType": 2},
    #             ]
    #         }
    #     },
    #     {
    #         "$project": {
    #             "Price": 1,
    #             "Volume": 1,
    #             "Total": {"$multiply": ["$Price", "$Volume"]},
    #             "TotalCommission": 1,
    #             "TradeItemBroker": 1,
    #             "Sell": {
    #                 "$subtract": [
    #                     {"$multiply": ["$Price", "$Volume"]},
    #                     "$TotalCommission",
    #                 ]
    #             },
    #         }
    #     },
    #     {
    #         "$group": {
    #             "_id": "$id",
    #             "TotalFee": {"$sum": "$TradeItemBroker"},
    #             "TotalSell": {"$sum": "$Sell"},
    #         }
    #     },
    #     {"$project": {"_id": 0, "TotalSell": 1, "TotalFee": 1}},
    # ]
    #
    # buy_agg_result = peek(database.trades.aggregate(pipeline=buy_pipeline))
    # sell_agg_result = peek(database.trades.aggregate(pipeline=sell_pipeline))
    #
    # marketer_total = {"TotalPureVolume": 0, "TotalFee": 0}
    #
    # buy_dict = {"vol": 0, "fee": 0}
    #
    # sell_dict = {"vol": 0, "fee": 0}
    #
    # if buy_agg_result:
    #     buy_dict["vol"] = buy_agg_result.get("TotalBuy")
    #     buy_dict["fee"] = buy_agg_result.get("TotalFee")
    #
    # if sell_agg_result:
    #     sell_dict["vol"] = sell_agg_result.get("TotalSell")
    #     sell_dict["fee"] = sell_agg_result.get("TotalFee")
    #
    # marketer_total["TotalPureVolume"] = buy_dict.get("vol") + sell_dict.get("vol")
    # marketer_total["TotalFee"] = buy_dict.get("fee") + sell_dict.get("fee")
    pure_fee = marketer_total.get("TotalFee") * 0.65
    marketer_fee = 0
    tpv = marketer_total.get("TotalPureVolume")
    b=plans
    cbt = contract_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})["CalculationBaseType"]
    for plan in plans[cbt]:
        plans[cbt][plan]['start']
        if plans[cbt][plan]['start'] <= tpv < plans[cbt][plan]['end']:
            marketer_fee = pure_fee * plans[cbt][plan]['marketer_share']
            plan_name = plan
            if plans[cbt][plan]['end'] == inf:
                next_plan = 0
            else:
                next_plan = plans[cbt][plan]['end'] - tpv
    final_fee = marketer_fee
    try:
        salary = contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})["Salary"] * marketer_fee
    except:
        salary = 0
    try:
        insurance = contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})["InsuranceCoefficient"] * marketer_fee
    except:
        insurance = 0
    try:
        tax = contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})["TaxCoefficient"] * marketer_fee
    except:
        tax = 0
    try:
        collateral = contded_coll.find_one({"MarketerID": args.MarketerID}, {"_id": False})["CollateralCoefficient"] * marketer_fee
    except:
        collateral = 0
    deductions = salary + insurance + tax + collateral
    additions = args.Collateral
    if args.Additions:
        additions = additions + args.Additions
    if args.Deductions:
        deductions = deductions + args.Deductions
    payment = final_fee + additions - deductions
    result = {
        "TotalPureVolume": marketer_total.get("TotalPureVolume"),
        "TotalFee": marketer_total.get("TotalFee"),
        "PureFee": int(pure_fee),
        "MarketerFee": int(marketer_fee),
        "Plan": plan,
        "Next Plan": next_plan,
        "Tax": int(tax),
        "Collateral of This Month": int(collateral),
        "Sum of Previous Collaterals": args.Collateral,
        "Sum of Additions": int(additions),
        "Sum of Deductions": int(deductions),
        "FinalFee": int(final_fee),
        "Payment": int(payment),

    }

    return ResponseOut(timeGenerated=datetime.now(), result=result, error="")


add_pagination(factors)