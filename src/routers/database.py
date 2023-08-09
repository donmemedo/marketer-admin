"""_summary_

Returns:
    _type_: _description_
"""
from datetime import datetime, date
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd
from fastapi.exceptions import RequestValidationError
# from src.tools.tokens import JWTBearer, get_role_permission
from src.tools.utils import check_permissions
from src.auth.authentication import get_role_permission
from src.tools.database import get_database
from src.schemas.database import ResponseListOut, CollectionRestore
from src.config import settings
from src.tools.logger import logger
import requests
from pymongo import MongoClient, errors
from src.auth.authorization import authorize


database = APIRouter(prefix="/database")


@database.put(
    "/get-customers",
    # dependencies=[Depends(JWTBearer())],
    tags=["Database"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Write",
        "MarketerAdmin.Database.Write",
        "MarketerAdmin.TBSSync.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.Database.Update",
        "MarketerAdmin.TBSSync.Update",
    ]
)
async def get_customers(
    request: Request,
    coll_ress: CollectionRestore,
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
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Write",
        "MarketerAdmin.Database.Write",
        "MarketerAdmin.TBSSync.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.Database.Update",
        "MarketerAdmin.TBSSync.Update",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    # brokerage = get_database()
    try:
        cus_collection = brokerage[settings.CUSTOMER_COLLECTION]
    except errors.CollectionInvalid as err:
        logger.error("No Connection to Collection", err)
    try:
        given_date = jd.strptime(coll_ress.date, "%Y-%m-%d").todate()
    except:
        logger.error("Given Date was in Invalid Format.")
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # }
        # return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # )

    cus_getter(date=given_date)
    logger.info(f"Updating Customers Database was requested by {user_id}")
    logger.info(
        "Ending Time of getting List of Registered Customers in %s: %s",
        coll_ress.date,
        jd.now(),
    )
    return ResponseListOut(
        result=[],
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@database.put(
    "/get-firms",
    # dependencies=[Depends(JWTBearer())],
    tags=["Database"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Write",
        "MarketerAdmin.Database.Write",
        "MarketerAdmin.TBSSync.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.Database.Update",
        "MarketerAdmin.TBSSync.Update",
    ]
)
async def get_firms(
    request: Request,
    coll_ress: CollectionRestore,
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
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Write",
        "MarketerAdmin.Database.Write",
        "MarketerAdmin.TBSSync.Write",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.Database.Update",
        "MarketerAdmin.TBSSync.Update",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    # brokerage = get_database()
    try:
        firm_collection = brokerage[settings.FIRMS_COLLECTION]
    except errors.CollectionInvalid as err:
        logger.error("No Connection to Collection", err)
    try:
        given_date = jd.strptime(coll_ress.date, "%Y-%m-%d").todate()
    except:
        logger.error("Given Date was in Invalid Format.")
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # }
        # return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # )

    firm_getter(date=given_date)
    logger.info(f"Updating Firms Database was requested by {user_id}")
    logger.info(
        "Ending Time of getting List of Registered Firms in %s: %s",
        coll_ress.date,
        jd.now(),
    )
    return ResponseListOut(
        result=[],
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@database.post(
    "/get-trades",
    # dependencies=[Depends(JWTBearer())],
    tags=["Database"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.Database.Create",
        "MarketerAdmin.TBSSync.Create",
    ]
)
async def get_trades(
    request: Request,
    coll_ress: CollectionRestore,
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
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.Database.Create",
        "MarketerAdmin.TBSSync.Create",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    # brokerage = get_database()
    try:
        trades_collection = brokerage[settings.TRADES_COLLECTION]
    except errors.CollectionInvalid as err:
        logger.error("No Connection to Collection", err)
    try:
        given_date = jd.strptime(coll_ress.date, "%Y-%m-%d").todate()
    except:
        logger.error("Given Date was in Invalid Format.")
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # }
        # return JSONResponse(status_code=412, content=resp)

        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # )

    trade_getter(date=given_date)
    logger.info(f"Updating Trades Database was requested by {user_id}")
    logger.info(
        "Ending Time of getting List of Trades in %s is: %s", coll_ress.date, jd.now()
    )
    return ResponseListOut(
        result=[],
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


@database.delete(
    "/delete-trades",
    # dependencies=[Depends(JWTBearer())],
    tags=["Database"],
    response_model=None,
)
@authorize(
    [
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.Database.Delete",
        "MarketerAdmin.TBSSync.Delete",
    ]
)
async def delete_trades(
    request: Request,
    args: CollectionRestore = Depends(CollectionRestore),
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
        "MarketerAdmin.All.All",
        "MarketerAdmin.Database.All",
        "MarketerAdmin.TBSSync.All",
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.Database.Delete",
        "MarketerAdmin.TBSSync.Delete",
    ]
    allowed = check_permissions(role_perm["roles"], permissions)
    if allowed:
        pass
    else:
        raise HTTPException(status_code=403, detail="Not authorized.")

    # brokerage = get_database()
    try:
        trades_collection = brokerage[settings.TRADES_COLLECTION]
    except errors.CollectionInvalid as err:
        logger.error("No Connection to Collection", err)
    try:
        given_date = jd.strptime(args.date, "%Y-%m-%d").todate()
    except:
        logger.error("Given Date was in Invalid Format.")
        raise RequestValidationError(TypeError, body={"code": "30090", "status": 412})
        # resp = {
        #     "result": [],
        #     "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     "error": {
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # }
        # return JSONResponse(status_code=412, content=resp)
        #
        # return ResponseListOut(
        #     result=[],
        #     timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        #     error={
        #         "message": "تاریخ اشتباه وارد شده است.",
        #         "code": "30090",
        #     },
        # )

    trades_collection.delete_many({"TradeDate": {"$regex": str(given_date)}})
    logger.info(f"Updating Trades Database was requested by {user_id}")
    logger.info(
        "Ending Time of deleting List of Trades in %s is: %s", args.date, jd.now()
    )
    return ResponseListOut(
        result=[],
        timeGenerated=jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        error="",
    )


def get_database():
    """Getting Database

    Returns:
        Database: Mongo Database
    """
    connection_sting = settings.MONGO_CONNECTION_STRING
    client = MongoClient(connection_sting)
    database = client[settings.MONGO_DATABASE]
    return database


def cus_getter(size=10, date="2023-01-31"):
    """Getting List of Customers in Actual Date

    Args:
        size (int, optional): Page Size in Pagination of Results. Defaults to 10.
        date (str, optional): Date. Defaults to "2023-01-31".
    """
    brokerage = get_database()
    cus_collection = brokerage[settings.CUSTOMER_COLLECTION]

    temp_req = requests.get(
        "https://tadbirwrapper.tavana.net/tadbir/GetCustomerList",
        params={"request.date": date, "request.pageIndex": 0, "request.pageSize": 1},
        timeout=100,
    )
    if temp_req.status_code != 200:
        logger.critical("Http response code: %s", temp_req.status_code)
        total_records = 0
    else:
        total_records = temp_req.json()["TotalRecords"]
    logger.info("\t  \t  \t  %s \t  \t  \t  %s", date, total_records)
    for page in range(0, total_records // 10 + 1):
        logger.info("Getting Page %d from %d pages", page + 1, total_records // 10 + 1)
        req = requests.get(
            "https://tadbirwrapper.tavana.net/tadbir/GetCustomerList",
            params={
                "request.date": date,
                "request.pageIndex": page,
                "request.pageSize": size,
            },
            timeout=100,
        )
        if req.status_code != 200:
            logger.critical("Http response code: %s", req.status_code)
            records = ""
        else:
            response = req.json()
            records = response.get("Result")
        for record in records:
            if record is None:
                logger.info("Record is empty.")
                continue
            try:
                cus_collection.insert_one(record)
                logger.info("Record %s added to Mongodb", record.get("PAMCode"))
            except errors.DuplicateKeyError as dup_error:
                logger.error("%s", dup_error)
                cus_collection.delete_one({"PAMCode": record.get("PAMCode")})
                cus_collection.insert_one(record)
                logger.info("Record %s was Updated", record.get("PAMCode"))

    logger.info("\n \n \n \t \t All were gotten!!!")
    logger.info("Time of getting List of Customers of %s is: %s", date, datetime.now())


def firm_getter(size=10, date="2023-01-31"):
    """Getting List of Firms in Actual Date

    Args:
        size (int, optional): Page Size in Pagination of Results. Defaults to 10.
        date (str, optional): Date. Defaults to "2023-01-31".
    """
    brokerage = get_database()
    firm_collection = brokerage[settings.FIRMS_COLLECTION]

    temp_req = requests.get(
        "https://tadbirwrapper.tavana.net/tadbir/GetFirmList",
        params={"request.date": date, "request.pageIndex": 0, "request.pageSize": 1},
        timeout=100,
    )
    if temp_req.status_code != 200:
        logger.critical("Http response code: %s", temp_req.status_code)
        total_records = 0
    else:
        total_records = temp_req.json()["TotalRecords"]
    logger.info("\t  \t  \t  %s \t  \t  \t  %s", date, total_records)
    for page in range(0, total_records // 10 + 1):
        logger.info("Getting Page %d from %d pages", page + 1, total_records // 10 + 1)
        req = requests.get(
            "https://tadbirwrapper.tavana.net/tadbir/GetFirmList",
            params={
                "request.date": date,
                "request.pageIndex": page,
                "request.pageSize": size,
            },
            timeout=100,
        )
        if req.status_code != 200:
            logger.critical("Http response code: %s", req.status_code)
            records = ""
        else:
            response = req.json()
            records = response.get("Result")
        for record in records:
            if record is None:
                logger.info("Record is empty.")
                continue
            try:
                firm_collection.insert_one(record)
                logger.info("Record %s added to Mongodb", record.get("PAMCode"))
            except errors.DuplicateKeyError as dup_error:
                logger.error("%s", dup_error)
                firm_collection.delete_one({"PAMCode": record.get("PAMCode")})
                firm_collection.insert_one(record)
                logger.info("Record %s was Updated", record.get("PAMCode"))

    logger.info("\n \n \n \t \t All were gotten!!!")
    logger.info("Time of getting List of Firms of %s is: %s", date, datetime.now())


def get_trades_list(page_size=50, page_index=0, selected_date="2022-12-31"):
    """Getting List of Trades in Selected Date

    Args:
        page_size (int, optional): Page Size in Pagination of Results. Defaults to 50.
        page_index (int, optional): Page Index in Pagination of Results. Defaults to 0.
        selected_date (str, optional): Selected Date. Defaults to "2022-12-31".

    Raises:
        RuntimeError: Server Error.

    Returns:
        Records: List of Trades in JSON.
    """
    req = requests.get(
        "https://tadbirwrapper.tavana.net/tadbir/GetDailyTradeList",
        params={
            "request.date": selected_date,
            "request.pageIndex": page_index,
            "request.pageSize": page_size,
        },
        timeout=100,
    )
    if req.status_code != 200:
        logger.critical("Http response code: %s", req.status_code)
        return "", 0
    response = req.json()
    return response.get("Result"), response.get("TotalRecords")


def trade_getter(date=date.today()):
    """Getting List of Trades in Today."""
    logger.info(datetime.now())
    brokerage = get_database()
    trades_collection = brokerage[settings.TRADES_COLLECTION]
    page_index = 0
    logger.info("Getting trades of %s", date)
    while True:
        response, total = get_trades_list(page_index=page_index, selected_date=date)
        if not response:
            logger.info("\t \t \t List is Empty!!!")
            break
        logger.info("Page %d of %d Pages", page_index + 1, 1 + total // 50)
        for record in response:
            try:
                trades_collection.insert_one(record)
                logger.info(
                    "Added: %s, %s, %s \n",
                    record.get("TradeNumber"),
                    record.get("TradeDate"),
                    record.get("MarketInstrumentISIN"),
                )
                logger.info(
                    "TradeNumber %s added to mongodb", record.get("TradeNumber")
                )
            except errors.DuplicateKeyError as dupp_error:
                logger.error("%s", dupp_error)
        logger.info("\t \t All were gotten!!!")
        page_index += 1
    logger.info("Time of getting List of Trades of %s is: %s", date, datetime.now())
