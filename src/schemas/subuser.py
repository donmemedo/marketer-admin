"""_summary_
"""
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Optional, Any, List, Dict

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
class SubUserIn:
    """_summary_"""

    first_name: str = Query("")
    last_name: str = Query("")
    register_date: str = Query("")
    phone: str = Query("")
    mobile: str = Query("")
    user_id: str = Query("")
    username: str = Query("")
    pamcode: str = Query("")
    page_size: int = Query(5)
    page_index: int = Query(0)


@dataclass
class SubCostIn:
    """_summary_"""

    first_name: str = Query("")
    last_name: str = Query("")
    phone: str = Query("")
    mobile: str = Query("")
    user_id: str = Query("")
    username: str = Query("")
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")


@dataclass
class UsersTotalPureIn:
    to_date: str = Query(default=None, alias="EndDate")
    from_date: str = Query(default=current_date, alias="StartDate")
    asc_desc_TPV: Optional[bool] = False
    asc_desc_TF: Optional[bool] = False
    asc_desc_LMTPV: Optional[bool] = False
    asc_desc_LMTF: Optional[bool] = False
    asc_desc_FN: Optional[bool] = False
    asc_desc_LN: Optional[bool] = False
    asc_desc_UC: Optional[bool] = False
    sorted: bool = False


@dataclass
class Pages:
    size: int = Query(10, alias="PageSize")
    page: int = Query(0, alias="PageNumber")


@dataclass
class UsersListIn(Pages):
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")


@dataclass
class TotalUsersListIn(Pages):
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")
    asc_desc_TPV: Optional[bool] = False
    asc_desc_TF: Optional[bool] = False
    sorted: bool = False


@dataclass
class MarketerIdpIdIn:
    """_summary_"""

    idpid: str


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


class UserTypeEnum(str, Enum):
    active = "active"
    inactive = "inactive"


class SortField(str, Enum):
    REGISTRATION_DATE = "RegisterDate"
    TotalPureVolume = "TotalPureVolume"


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1
