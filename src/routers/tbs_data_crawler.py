import datetime
import json
from src.tools.statics import statics
from src.tools.queries import *
import requests
from fastapi import Depends, status, APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError
from src.tools import messages
from src.config import *
from src.tools.database import get_database
from src.tools.logger import logger
from src.schemas.tbs_data_crawler import *
from khayyam import JalaliDatetime
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize


tbs_data_crawler = APIRouter(prefix="/tbs")


class Cookie:
    def __init__(self, cookie_value=None):
        self.cookie = cookie_value


cookie = Cookie()


@tbs_data_crawler.post("/cookie", tags=["TBS - Cookie"])
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.TBSSync.Read",
        "MarketerAdmin.TBSSync.Delete",
        "MarketerAdmin.TBSSync.Create",
        "MarketerAdmin.TBSSync.Update",
        "MarketerAdmin.TBSSync.All",
    ]
)
async def set_cookie(
    args: CookieIn = Depends(CookieIn),
    role_perm: dict = Depends(get_role_permission),
):
    cookie.cookie = args.cookie_value

    return cookie.cookie


@tbs_data_crawler.get("/cookie", tags=["TBS - Cookie"])
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.TBSSync.Read",
        "MarketerAdmin.TBSSync.Delete",
        "MarketerAdmin.TBSSync.Create",
        "MarketerAdmin.TBSSync.Update",
        "MarketerAdmin.TBSSync.All",
    ]
)
async def get_cookie(
    role_perm: dict = Depends(get_role_permission),
):
    return cookie.cookie


@tbs_data_crawler.delete("/trades", tags=["TBS - Trades"])
@authorize(
    [
        "MarketerAdmin.All.Delete",
        "MarketerAdmin.All.All",
        "MarketerAdmin.TBSSync.Delete",
        "MarketerAdmin.TBSSync.All",
    ]
)
async def delete_trades(
    args: DeleteTradesIn = Depends(DeleteTradesIn),
    db: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    try:
        db.trades.delete_many(
            {"TradeDate": {"$regex": args.trade_date.strftime(settings.DATE_STRING)}}
        )
        logger.info(f"All trades has been deleted for {args.trade_date}")
        return ResponseOut(
            error="داده‌ها با موفقیت از سیستم حذف شد",
            result=[],
            timeGenerated=datetime.now(),
        )
    except Exception:
        logger.error("Error while delete data in database")
        logger.exception("Error while delete data in database")
        return jsonable_encoder(
            JSONResponse(
                status_code=500,
                content=ResponseOut(
                    error=messages.HTTP_500_ERROR,
                    result=[],
                    timeGenerated=datetime.now(),
                ),
            )
        )


@tbs_data_crawler.get("/trades", tags=["TBS - Trades"])
@authorize(
    [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.TBSSync.Create",
        "MarketerAdmin.TBSSync.Update",
        "MarketerAdmin.TBSSync.All",
    ]
)
async def get_trades(
    args: TradesIn = Depends(TradesIn),
    db: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    try:
        response = requests.post(
            settings.TBS_TRADES_URL,
            headers=tbs_trades_header(args.cookie),
            data=tbs_trades_payload(
                args.trade_date.year, args.trade_date.month, args.trade_date.day
            ),
        )

        response.raise_for_status()

        if "<html>".encode() in response.content:
            raise Exception()

    except Exception as e:
        return JSONResponse(
            status_code=599,
            content=jsonable_encoder(
                ResponseOut(
                    error=messages.CONNECTION_FAILED,
                    result=[],
                    timeGenerated=datetime.now(),
                )
            ),
        )

    response = json.loads(response.content)
    logger.info(f"On {args.trade_date}, number of records are: {response.get('total')}")

    if response.get("total", 0) != 0:
        trade_records = response.get("data")
        duplicates = 0
        buys = 0
        sells = 0
        inserted = []
        try:
            for trade in trade_records:
                # total commission
                trade["TotalCommission"] = trade["TradeItemTotalCommission"]

                # set trade type
                if trade["TradeSideTitle"] == "فروش":
                    trade["TradeType"] = 2
                    sells += 1
                else:
                    trade["TradeType"] = 1
                    buys += 1

                # set ISIN
                trade["MarketInstrumentISIN"] = trade["ISIN"]

                # set symbol
                trade["TradeSymbol"] = trade["Symbol"]
                try:
                    db.tradesbackup.insert_one(trade)
                    inserted.append(trade)
                except DuplicateKeyError as dup_error:
                    logger.info("Duplicate record", {"error": dup_error})
                    duplicates += 1
                    if trade["TradeType"] == 2:
                        sells -= 1
                    else:
                        buys -= 1
                logger.info(f"Successfully get trade records of  {args.trade_date}")

            result = {}
            # if results:
            result["InsertedTradeCount"] = len(trade_records) - duplicates
            result["errorCode"] = None
            result["errorMessage"] = None
            result["InsertedBuyTradeCount"] = buys
            result["InsertedSellTradeCount"] = sells
            result["InsertedTradeCodeCount"] = len(
                {v["TradeCode"]: v for v in inserted}
            )
            result["TradeDate"] = JalaliDatetime.now().date().isoformat()
            resp = {
                "result": result,
                "GeneratedDateTime": datetime.now(),
                "error": {
                    "message": "Null",
                    "code": "Null",
                },
            }

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED, content=jsonable_encoder(resp)
            )
        except BulkWriteError as e:
            for error in e.details.get("writeErrors"):
                if error.get("code") != 11000:
                    logger.error("Bulk Write Error")
                    logger.exception("Bulk Write Error")
                    return JSONResponse(
                        status_code=status.HTTP_418_IM_A_TEAPOT,
                        content=jsonable_encoder(
                            ResponseOut(
                                error=messages.BULK_WRITE_ERROR,
                                result=[],
                                timeGenerated=datetime.now(),
                            )
                        ),
                    )
    else:
        logger.info("No trade record found. List is empty.")
        return ResponseOut(
            error=messages.NO_TRADES_ERROR, result=[], timeGenerated=datetime.now()
        )


@tbs_data_crawler.get("/customers", tags=["TBS - Customers"], response_model=None)
@authorize(
    [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.Update",
        "MarketerAdmin.All.All",
        "MarketerAdmin.TBSSync.Create",
        "MarketerAdmin.TBSSync.Update",
        "MarketerAdmin.TBSSync.All",
    ]
)
async def get_customers(
    args: CustomersIn = Depends(CustomersIn),
    db: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    if args.register_date:
        register_date = args.register_date.strftime(statics.DATE_FORMAT)
    else:
        register_date = None

    if args.modified_date:
        modified_date = args.modified_date.strftime(statics.DATE_FORMAT)
    else:
        modified_date = None

    try:
        response = requests.get(
            settings.TBS_CUSTOMERS_URL,
            params=tbs_customer_filter_params(register_date, modified_date),
            headers=tbs_customer_header(cookie.cookie),
        )

        response.raise_for_status()

        if "<html>".encode() in response.content:
            raise Exception()

    except Exception as e:
        return JSONResponse(
            status_code=599,
            content=jsonable_encoder(
                ResponseOut(
                    error=messages.CONNECTION_FAILED,
                    result=[],
                    timeGenerated=datetime.now(),
                )
            ),
        )

    response = json.loads(response.content)
    logger.info(f"Number of records are: {response.get('total')}")

    if response.get("total", 0) != 0:
        records = response.get("data")
        results = []
        privates = legals = newp = newl = updp = updl = 0
        for record in records:
            if record["PartyTypeTitle"] == statics.PRIVATE_USER:
                record["CustomerType"] = 1
                privates += 1
            else:
                record["CustomerType"] = 2
                legals += 1
            record["BrokerBranch"] = record["BrokerBranchTitle"]
            record["DetailLedgerCode"] = record["AccountCodes"]
            record["Email"] = record["UserEmail"]
            record["IDNumber"] = record["BirthCertificateNumber"]
            record["NationalCode"] = record["NationalIdentification"]
            record["PAMCode"] = record["TradeCodes"]
            record["BourseCode"] = record["BourseCodes"]
            record["Referer"] = record["RefererTitle"]
            record["Username"] = record["UserName"]
            if not record["Mobile"]:
                record["Mobile"] = record["Phones"]
            try:
                db.customerzs.insert_one(record)
                if record["CustomerType"] == 1:
                    newp += 1
                else:
                    newl += 1
                logger.info(
                    f"Successfully get Customers in {datetime.now().isoformat()} "
                )
            except DuplicateKeyError as e:
                if e.details.get("code") == 11000:
                    logger.error(
                        f"Duplicate Key Error for {record.get('FirstName')} {record.get('LastName')}"
                    )
                    record.pop("_id")
                    if (
                        db.customerzs.find_one(
                            {"PAMCode": record.get("PAMCode")}, {"_id": False}
                        )
                        != record
                    ):
                        db.customerzs.delete_one({"PAMCode": record.get("PAMCode")})
                        db.customerzs.insert_one(record)
                        if record["CustomerType"] == 1:
                            updp += 1
                        else:
                            updl += 1
                        record.pop("_id")
                        results.append(record)
                        logger.info(
                            f"Record {record.get('FirstName')} {record.get('LastName')} was Updated"
                        )
                else:
                    logger.error("Bulk Write Error")
                    logger.exception("Bulk Write Error")
                    return JSONResponse(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        content=jsonable_encoder(
                            ResponseOut(
                                error=messages.BULK_WRITE_ERROR,
                                result=[],
                                timeGenerated=datetime.now(),
                            )
                        ),
                    )
        result = {}
        if results:
            result["pagedData"] = results
            result["errorCode"] = None
            result["errorMessage"] = None
            result["AllCustomerCount"] = len(records)
            result["AllPrivateCustomerCount"] = privates
            result["AllLegalCustomerCount"] = legals
            result["AllNewPrivateCustomerCount"] = newp
            result["AllNewLegalCustomerCount"] = newl
            result["AllUpdatedPrivateCustomerCount"] = updp
            result["AllUpdatedLegalCustomerCount"] = updl
            result["Date"] = JalaliDatetime.now().date().isoformat()
            resp = {
                "result": result,
                "GeneratedDateTime": datetime.now(),
                "error": {
                    "message": "Null",
                    "code": "Null",
                },
            }

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED, content=jsonable_encoder(resp)
            )
        else:
            result["errorCode"] = None
            result["errorMessage"] = messages.SUCCESSFULLY_WRITE_DATA
            result["AllCustomersCount"] = len(records)
            result["AllPrivateCustomerCount"] = privates
            result["AllLegalCustomerCount"] = legals
            result["AllNewPrivateCustomerCount"] = newp
            result["AllNewLegalCustomerCount"] = newl
            result["Date"] = JalaliDatetime.now().date().isoformat()
            resp = {
                "result": result,
                "GeneratedDateTime": datetime.now(),
                "error": {
                    "message": messages.SUCCESSFULLY_WRITE_DATA,
                    "code": "Null",
                },
            }

            return JSONResponse(
                status_code=status.HTTP_201_CREATED, content=jsonable_encoder(resp)
            )
    else:
        logger.info("No records found. List is empty.")
        return ResponseOut(
            error=messages.NO_RECORDS_ERROR, result=[], timeGenerated=datetime.now()
        )


@tbs_data_crawler.get("/reconciliation", tags=["TBS - Reconciliation"])
@authorize(
    [
        "MarketerAdmin.All.Read",
        "MarketerAdmin.All.All",
        "MarketerAdmin.TBSSync.Read",
        "MarketerAdmin.TBSSync.All",
    ]
)
async def reconciliation(
    args: ReconciliationIn = Depends(ReconciliationIn),
    db: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    temp_trades_coll = db["temptrades"]
    trades_coll = db[settings.TRADES_COLLECTION]
    marketer_coll = db[settings.MARKETER_COLLECTION]
    customer_coll = db[settings.CUSTOMER_COLLECTION]

    if args.MarketerID:
        marketers = marketer_coll.find_one({"Id": args.MarketerID}, {"_id": False})
    else:
        marketerrs = marketer_coll.find(
            {"TbsReagentId": {"$exists": True, "$not": {"$size": 0}}},
            {"_id": False},
        )
        marketers = dict(enumerate(marketerrs))
    results = []
    dates = next(
        temp_trades_coll.aggregate(
            [
                {
                    "$group": {
                        "_id": 0,
                        "max": {"$max": "$TradeDate"},
                        "min": {"$min": "$TradeDate"},
                    }
                }
            ]
        ),
        {"max": "2030-12-29T23:59:59", "min": "2015-01-01T00:00:01"},
    )
    from_date = datetime.fromisoformat(dates["min"]).date().isoformat()
    if args.start_date and (from_date < args.start_date.isoformat()):
        from_date = args.start_date.isoformat()
    to_date = datetime.fromisoformat(dates["max"]).date().isoformat()
    if args.end_date and (
        args.end_date.replace(day=args.end_date.day + 1).isoformat() < to_date
    ):
        to_date = args.end_date.replace(day=args.end_date.day + 1).isoformat()

    for num in marketers:
        marketer = marketers[num]
        query = {"Referer": marketer["TbsReagentName"]}
        fields = {"PAMCode": 1}

        customers_records = customer_coll.find(query, fields)
        trade_codes = [c.get("PAMCode") for c in customers_records]
        pipeline = [
            filter_users_stage(trade_codes, from_date, to_date),
            project_commission_stage(),
            group_by_total_stage("id"),
            project_pure_stage(),
        ]
        diff_marketer_total = {"TotalTurnOver": 0, "TotalBrokerCommission": 0}
        tbs_marketer_total = next(
            temp_trades_coll.aggregate(pipeline=pipeline),
            {"TotalPureVolume": 0, "TotalFee": 0},
        )
        db_marketer_total = next(
            trades_coll.aggregate(pipeline=pipeline),
            {"TotalPureVolume": 0, "TotalFee": 0},
        )
        # if tbs_marketer_total == db_marketer_total:
        #     pass
        # else:
        tbs_marketer_total["BuyTradeCount"] = temp_trades_coll.count_documents(
            {
                "$and": [
                    {"TradeType": 1},
                    {"Referer": marketer["TbsReagentName"]},
                ]
            }
        )
        tbs_marketer_total["SellTradeCount"] = temp_trades_coll.count_documents(
            {
                "$and": [
                    {"TradeType": 2},
                    {"Referer": marketer["TbsReagentName"]},
                ]
            }
        )
        db_marketer_total["BuyTradeCount"] = trades_coll.count_documents(
            filter_trades(trade_codes, from_date, to_date, 1)
        )
        db_marketer_total["SellTradeCount"] = trades_coll.count_documents(
            filter_trades(trade_codes, from_date, to_date, 2)
        )
        diff_marketer_total["BuyTradeCount"] = (
            db_marketer_total["BuyTradeCount"] - tbs_marketer_total["BuyTradeCount"]
        )
        diff_marketer_total["SellTradeCount"] = (
            db_marketer_total["SellTradeCount"] - tbs_marketer_total["SellTradeCount"]
        )
        diff_marketer_total["TotalTurnOver"] = (
            db_marketer_total["TotalPureVolume"] - tbs_marketer_total["TotalPureVolume"]
        )
        diff_marketer_total["TotalBrokerCommission"] = (
            db_marketer_total["TotalFee"] - tbs_marketer_total["TotalFee"]
        )
        reconciliation_report = {
            "MarketerName": marketer["TbsReagentName"],
            "TBSBuyTradeCount": tbs_marketer_total["BuyTradeCount"],
            "APPBuyTradeCount": db_marketer_total["BuyTradeCount"],
            "TBSSellTradeCount": tbs_marketer_total["SellTradeCount"],
            "APPSellTradeCount": db_marketer_total["SellTradeCount"],
            "TBSTotalTradeCount": tbs_marketer_total["BuyTradeCount"]
            + tbs_marketer_total["SellTradeCount"],
            "APPTotalTradeCount": db_marketer_total["BuyTradeCount"]
            + db_marketer_total["SellTradeCount"],
            "APPSumOfActualTotalTurnOver": db_marketer_total["TotalPureVolume"],
            "TBSSumOfActualTotalTurnOver": tbs_marketer_total["TotalPureVolume"],
            "APPSumOfActualTotalBrokerCommission": db_marketer_total["TotalFee"],
            "TBSSumOfActualTotalBrokerCommission": tbs_marketer_total["TotalFee"],
            "DiffBuyTradeCount": diff_marketer_total["SellTradeCount"],
            "DiffSellTradeCount": diff_marketer_total["SellTradeCount"],
            "DiffTotalTradeCount": diff_marketer_total["BuyTradeCount"]
            + diff_marketer_total["SellTradeCount"],
            "DiffSumOfActualTotalTurnOver": diff_marketer_total["TotalTurnOver"],
            "DiffSumOfActualTotalBrokerCommission": diff_marketer_total[
                "TotalBrokerCommission"
            ],
        }
        results.append(reconciliation_report)

    msg = f"Reconciliation Report is constructed for dates between {from_date} and {to_date}"
    result = {
        "errorCode": None,
        "errorMessage": None,
        "pagedData": results,
        "ReportDates": msg,
    }
    resp = {
        "result": result,
        "GeneratedDateTime": datetime.now(),
        "error": {
            "message": "Null",
            "code": "Null",
        },
    }
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED, content=jsonable_encoder(resp)
    )


def tbs_trades_header(cookie):
    return {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": cookie,
        "Origin": "https://tbs.onlinetavana.ir",
        "Referer": "https://tbs.onlinetavana.ir/ClearingSettlement/ClrsReport/StockTradeSummary?FY=24&_dc=1687751379968",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


def tbs_trades_payload(year, month, day):
    return (
        f"BranchId=&DateFilter.StartDate={month}%2F{day}"
        f"%2F{year}&DateFilter.EndDate={month}"
        f"%2F{day}%2F{year}"
        f"&TradeState=All&TradeSide=Both&CustomerType=All&Bourse=All"
        f"&MaxWage=%D9%87%D8%B1%D8%AF%D9%88&MinWage=%D9%87%D8%B1%D8%AF%D9%88"
        f"&MarketInstrumentType=All&StockTradeSummaryReportType=Simple&"
        f"CounterPartyType=NoneOfThem&page=1&start=0&limit=2147483647&FY=24"
    )


def reconciliation_payload(
    start_year, start_month, start_day, end_year, end_month, end_day
):
    return (
        f"BranchId=&DateFilter.StartDate={start_month}%2F{start_day}"
        f"%2F{start_year}&DateFilter.EndDate={end_month}"
        f"%2F{end_day}%2F{end_year}"
        f"&TradeState=All&TradeSide=Both&CustomerType=All&Bourse=All"
        f"&MaxWage=%D9%87%D8%B1%D8%AF%D9%88&MinWage=%D9%87%D8%B1%D8%AF%D9%88"
        f"&MarketInstrumentType=All&StockTradeSummaryReportType=Simple&"
        f"CounterPartyType=NoneOfThem&page=1&start=0&limit=2147483647&FY=24"
    )


def tbs_portfolio_header(cookie):
    return {
        "Accept": "*/*",
        # 'Accept-Encoding': 'gzip, deflate, br',
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Referer": "https://tbs.onlinetavana.ir/CustomerManagement/Customer/PortfolioIndex?FY=24&_dc=1691393987026",
        "Cookie": cookie,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }


def tbs_portfolio_params():
    return {
        "_dc": "1691399042865",
        "action": "read",
        "Filter.TradeSystem": "TSETradingSystem",
        "filterheader": "{}",
        "page": "1",
        "start": "0",
        "limit": "500",
        "FY": "24",
    }


def tbs_customer_header(cookie):
    return {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Referer": "https://tbs.onlinetavana.ir/CustomerManagement/Customer?tradeSystem=TSETradingSystem",
        "Host": "tbs.onlinetavana.ir",
        "Cookie": cookie,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Site": "same-origin",
    }


def tbs_customer_params():
    return {
        "_dc": "1688973946480",
        "action": "read",
        "Filter.TradeSystem": "TSETradingSystem",
        "Filter.ShowFastList": "false",
        "filterheader": "{}",
        "page": "1",
        "start": "0",
        "limit": "80000",
        "sort": '[{"property":"TradeCodes","direction":"ASC"}]',
        "FY": "24",
    }


def tbs_customer_filter_params(register_date=None, modified_date=None):
    response = {
        "_dc": "1688973946480",
        "action": "read",
        "Filter.TradeSystem": "TSETradingSystem",
        "Filter.ShowFastList": "false",
        "page": "1",
        "start": "0",
        "limit": "80000",
        "sort": '[{"property":"TradeCodes","direction":"ASC"}]',
        "FY": "24",
    }

    if register_date:
        response["filterheader"] = (
            '{"RegisterDate": {"type": "date", "op": "=", "value": "'
            + register_date
            + '"}}'
        )
    elif modified_date:
        response["filterheader"] = (
            '{"ModifiedDate":{"type":"date","op":"=","value":"' + modified_date + '"}}'
        )

    return response
