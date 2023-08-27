"""_summary_
"""
from dataclasses import dataclass
from typing import Optional, Any, List, Dict
from enum import Enum, IntEnum
from fastapi import Query
from pydantic import BaseModel
from khayyam import JalaliDatetime


current_date = JalaliDatetime.today().replace(day=1).strftime("%Y-%m-%d")
current_month = JalaliDatetime.today().month
current_year = JalaliDatetime.today().year


@dataclass
class ModifyMarketerIn:
    CurrentIdpId: str
    InvitationLink: Optional[str] = None
    FirstName: Optional[str] = None
    LastName: Optional[str] = None
    RefererType: Optional[str] = None
    NewIdpId: Optional[str] = None
    NationalID: Optional[int] = None


from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Dict, List

from fastapi import Query
from khayyam import JalaliDatetime
from datetime import date

current_date = (
    date.today().isoformat()
)  # JalaliDatetime.today().replace(day=1).strftime("%Y-%m-%d")


@dataclass
class UserSearchIn:
    name: str = Query("", alias="Name")
    page_index: int = Query(1, alias="PageNumber")
    page_size: int = Query(5, alias="PageSize")


@dataclass
class UserTotalIn:
    trade_code: str = Query(alias="TradeCode")
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")


@dataclass
class MarketerTotalIn:
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")


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
    timeGenerated: JalaliDatetime
    result: Dict
    error: str = Query("nothing")


@dataclass
class UserTotalOutList:
    result: dict[UserTotalOut]


@dataclass
class CostIn:
    insurance: int = Query(0, alias="Insurance")
    tax: int = Query(0, alias="Tax")
    salary: int = Query(0, alias="Salary")
    collateral: int = Query(0, alias="Collateral")
    from_date: str = Query(current_date, alias="StartDate")
    to_date: str = Query(current_date, alias="EndDate")


@dataclass
class MarketerInvitationIn:
    id: int
    invitation_link: str


@dataclass
class Pages:
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")


class UserTypeEnum(str, Enum):
    active = "active"
    inactive = "inactive"


class SortField(str, Enum):
    REGISTRATION_DATE = "RegisterDate"
    TotalPureVolume = "TotalPureVolume"


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1


@dataclass
class UsersListIn(Pages):
    sort_by: SortField = Query(SortField.REGISTRATION_DATE, alias="SortBy")
    sort_order: SortOrder = Query(SortOrder.ASCENDING, alias="SortOrder")
    user_type: UserTypeEnum = Query(UserTypeEnum.active, alias="UserType")
    from_date: str = Query(current_date, alias="StartDate")
    to_date: str = Query(current_date, alias="EndDate")


@dataclass
class FactorIn:
    insurance: int = Query(0)
    tax: int = Query(0)
    salary: int = Query(0)
    collateral: int = Query(0)
    month: str = Query(current_date)
    year: str = Query(current_date)


@dataclass
class GetMarketerList:
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")
    IdpID: Optional[str] = Query("")


@dataclass
class GetCostIn:
    IdpID: Optional[str] = Query("")
    insurance: int = Query(0, alias="Insurance")
    tax: int = Query(0, alias="Tax")
    salary: int = Query(0, alias="Salary")
    collateral: int = Query(0, alias="Collateral")
    from_date: str = Query(current_date, alias="StartDate")
    to_date: str = Query(current_date, alias="EndDate")
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")


@dataclass
class GetFactorIn:
    IdpID: Optional[str] = Query("")
    insurance: int = Query(0)
    tax: int = Query(0)
    salary: int = Query(0)
    collateral: int = Query(0)
    month: str = Query(current_month)
    year: str = Query(current_year)
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")
