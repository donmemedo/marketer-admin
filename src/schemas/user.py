from fastapi import Query
from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Optional, Any, List, Dict
from khayyam import JalaliDatetime
from enum import Enum, IntEnum

current_date = JalaliDatetime.today().replace(day=1).strftime("%Y-%m-%d")
current_month = JalaliDatetime.today().month
current_year = JalaliDatetime.today().year


@dataclass
class UserTotalOut:
    TotalPureVolume: float
    TotalFee: float

class MarketerOut(BaseModel):
    FirstName: str
    LastName: str
    InvitationLink: Optional[str]
    RefererType: str
    CreateDate: str
    ModifiedBy: str
    CreatedBy: str
    ModifiedDate: str
    IdpId: Optional[str]
    Mobile: Optional[str]


@dataclass
class MarketersProfileIn:
    first_name: str = Query("")
    last_name: str = Query("")
    mobile: int = Query("")
    register_date: str = Query("")


@dataclass
class ModifyMarketerIn:
    CurrentIdpId: str
    InvitationLink: Optional[str] = Query("")
    Mobile: Optional[str] = Query("")
    FirstName: Optional[str] = Query("")
    LastName: Optional[str] = Query("")
    RefererType: Optional[str] = Query("")
    CreatedBy: Optional[str] = Query("")
    ModifiedDate: Optional[str] = Query("")
    NewIdpId: Optional[str] = Query("")
    Phone: Optional[str] = Query("")
    ID: Optional[str] = Query("")
    NationalID: Optional[str] = Query("")


@dataclass
class UserTradesIn:
    TradeCode: str = Query(alias="TradeCode")
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")




class UserTradesOut(BaseModel):
    AddedValueTax: str
    TotalCommission: str
    TradeType: str
    TradeCode: str
    TradeItemRayanBourse: str
    TradeItemBroker: str
    Volume: str
    BranchID: str
    Price: str
    MarketInstrumentISIN: str
    BranchTitle: str
    TradeDate: str
    BondDividend: str
    InstrumentCategory: str
    TradeNumber: str
    TransferTax: str
    Discount: str
    TradeSymbol: str
    TradeStationType: str


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
    marketername: str = Query(default=None)
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")


@dataclass
class UsersTotalPureIn:
    # HACK: because Pydantic do not support Jalali Date, I had to use the universal calendar.
    # to_date: str = Query("1401-12-01")
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
class MarketerIn:
    IdpID: str = None


class ConstOut(BaseModel):
    MarketerID: str
    LastName: str
    FirstName: str
    FixIncome: int
    Insurance: float
    Collateral: float
    Tax: float


@dataclass
class ModifyConstIn:
    MarketerID: str = None
    FixIncome: Optional[int] = 0
    Insurance: Optional[float] = 0
    Collateral: Optional[float] = 0.05
    Tax: Optional[float] = 0.1


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
class UserTotalOut:
    TotalPureVolume: float
    TotalFee: float
