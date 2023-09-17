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
class ModifyMarketerIn:
    CurrentIdpId: str
    InvitationLink: Optional[str] = None
    FirstName: Optional[str] = None
    LastName: Optional[str] = None
    RefererType: Optional[str] = None
    NewIdpId: Optional[str] = None
    NationalID: Optional[str] = None


@dataclass
class AddMarketerIn:
    CurrentIdpId: str
    InvitationLink: Optional[str] = None
    FirstName: Optional[str] = None
    LastName: Optional[str] = None
    RefererType: Optional[str] = None
    NationalID: Optional[str] = None


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
class MarketersProfileIn:
    # first_name: str = Query("")
    Title: str = Query(default=None, alias="TbsReagentName")
    UniqueId: str = Query(default=None)
    # last_name: str = Query("")
    Mobile: str = Query(default=None)
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")

    # register_date: str = Query("")


@dataclass
class MarketerIn:
    IdpID: str = None


@dataclass
class DiffTradesIn:
    IdpID: str = Query(alias="IdpID")
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
    result: Dict
    timeGenerated: JalaliDatetime
    error: str = Query("nothing")


@dataclass
class MarketerRelations:
    LeaderMarketerID: str
    FollowerMarketerID: str
    CommissionCoefficient: float
    StartDate: str = Query(default=current_date)
    EndDate: str = Query(default=None)


@dataclass
class DelMarketerRelations:
    LeaderMarketerID: str
    FollowerMarketerID: str


@dataclass
class SearchMarketerRelations:
    LeaderMarketerName: str = None
    LeaderMarketerID: str = None
    FollowerMarketerName: str = None
    FollowerMarketerID: str = None
    StartDate: str = Query(default="2021-01-01")
    EndDate: str = Query(default="2521-12-29")


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


class UserTypeEnum(str, Enum):
    active = "active"
    inactive = "inactive"


@dataclass
class UsersListIn(Pages):
    IdpID: str = Query(alias="IdpID")
    sort_by: SortField = Query(SortField.REGISTRATION_DATE, alias="SortBy")
    sort_order: SortOrder = Query(SortOrder.ASCENDING, alias="SortOrder")
    user_type: UserTypeEnum = Query(UserTypeEnum.active, alias="UserType")
    from_date: str = Query(date.today().isoformat(), alias="StartDate")
    to_date: str = Query(date.today().isoformat(), alias="EndDate")
