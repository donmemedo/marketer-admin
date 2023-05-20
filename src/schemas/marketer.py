from fastapi import Query
from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Optional, Any, List, Dict
from enum import Enum, IntEnum
from khayyam import JalaliDatetime


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
    InvitationLink: Optional[str] = None
    FirstName: Optional[str] = None
    LastName: Optional[str] = None
    RefererType: Optional[str] = None
    CreatedBy: Optional[str] = None
    ModifiedDate: Optional[str] = None
    CreateDate: Optional[str] = None
    ModifiedBy: Optional[str] = None
    NewIdpId: Optional[str] = None
    NationalID: Optional[int] = None


@dataclass
class UserTradesIn:
    TradeCode: str = Query(alias="TradeCode")


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
class ModifyFactorIn:

    MarketerID: str
    TotalPureVolume: int = None
    TotalFee: int = None
    PureFee: int = None
    MarketerFee: int = None
    Plan: str = None
    Tax: int = None
    Collateral: int = None
    FinalFee: int = None
    Payment: int = None
    FactorStatus: int = None
    Period: Optional[str] = str(current_year) + f"{current_month:02}"


@dataclass
class MarketerRelations:

    LeaderMarketerID: str
    FollowerMarketerID: str
    CommissionCoefficient: float
    StartDate: str = Query(default=current_date)
    EndDate: str = Query(default=None)
    # CreateDate: str = Query(default=current_date)
    # UpdateDate: str = Query(default=current_date)

@dataclass
class SearchMarketerRelations:

    LeaderMarketerName: str = None
    LeaderMarketerID: str = None
    FollowerMarketerName: str = None
    FollowerMarketerID: str = None
    StartDate: str = Query(default="1301-01-01")
    EndDate: str = Query(default="1501-12-29")
    CreateDate: str = Query(default=None)
    # UpdateDate: str = Query(default=current_date)


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
