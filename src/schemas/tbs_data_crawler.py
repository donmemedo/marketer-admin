from pydantic import BaseModel
from datetime import date, datetime
from typing import List, Any, Optional
from fastapi import Query
from dataclasses import dataclass


@dataclass
class TradesIn:
    trade_date: date = Query(default=date.today(), alias="TradeDate")
    cookie: str = Query(alias="Cookie")


@dataclass
class PortfolioIn:
    cookie: str = Query(alias="Cookie")


@dataclass
class DeleteTradesIn:
    trade_date: date = Query(default=date.today(), alias="TradeDate")


class ResponseOut(BaseModel):
    timeGenerated: datetime
    result: List[TradesIn] = List[Any]
    error: str


@dataclass
class CustomersIn:
    register_date: Optional[date] = Query(alias="RegisterDate", default=None)
    modified_date: Optional[date] = Query(alias="ModifiedDate", default=None)


@dataclass
class ReconciliationIn:
    MarketerID: Optional[str] = Query(alias="MarketerID", default=None)
    start_date: Optional[date] = Query(alias="StartDate", default=None)
    end_date: Optional[date] = Query(alias="EndDate", default=None)


@dataclass
class CookieIn:
    cookie_value: str
