"""_summary_
"""
from dataclasses import dataclass
from typing import Optional, Any, List, Dict
from enum import Enum, IntEnum
from pydantic import BaseModel
from fastapi import Query
from khayyam import JalaliDatetime


current_date = JalaliDatetime.today().replace(day=1).strftime("%Y-%m-%d")
current_month = JalaliDatetime.today().month
current_year = JalaliDatetime.today().year
from datetime import date

current_date = (
    date.today().isoformat()
)  # JalaliDatetime.today().replace(day=1).strftime("%Y-%m-%d")


@dataclass
class ModifyMarketerRefCodeIn:
    MarketerID: str
    ID: str = None
    SubsidiaryCode: str = None
    SubsidiaryTitle: str = None
    BranchCode: str = None
    BranchTitle: str = None
    RefCode: str = None
    Type: str = None
    Title: str = None


@dataclass
class UserTotalOut:
    TotalPureVolume: float
    TotalFee: float


@dataclass
class ResponseOut:
    timeGenerated: JalaliDatetime
    result: List[UserTotalOut] = List[Any]
    error: str = Query("nothing")


@dataclass
class ResponseListOut:
    result: Dict
    timeGenerated: JalaliDatetime
    error: str = Query("nothing")


@dataclass
class SearchMarketerRefCodeIn:
    MarketerID: str = Query("")
    ID: str = None
    SubsidiaryCode: str = None
    SubsidiaryTitle: str = None
    BranchCode: str = None
    BranchTitle: str = None
    RefCode: str = None
    Type: str = None
    Title: str = Query("")


@dataclass
class DelMarketerRefCodeIn:
    MarketerID: str


@dataclass
class Pages:
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")


class SortField(str, Enum):
    REGISTRATION_DATE = "RegisterDate"
    TotalPureVolume = "TotalPureVolume"


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1
