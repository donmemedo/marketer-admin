from fastapi import Query
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Optional




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
    last_name:  str = Query("")
    mobile:  int = Query("")
    register_date:  str = Query("")

@dataclass
class ModifyMarketerIn:
    IdpId: str
    InvitationLink: Optional[str] = Query("")
    Mobile: Optional[str] = Query("")


@dataclass
class UserTradesIn:
    TradeCode: str


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
    size: int = Query(10)
    page: int = Query(1)


@dataclass
class UsersListIn(Pages):
    from_date: str = Query("1401-12-01")
    to_date: str = Query("1401-12-01")


@dataclass
class UsersTotalPureIn:
    # HACK: because Pydantic do not support Jalali Date, I had to use the universal calendar.
    # to_date: str = Query("1401-12-01")
    to_date: str = None
    from_date: str = Query("1401-12-01")
    asc_desc_TPV: Optional[bool] = False
    asc_desc_TF: Optional[bool] = False
    asc_desc_LMTPV: Optional[bool] = False
    asc_desc_LMTF: Optional[bool] = False
    asc_desc_FN: Optional[bool] = False
    asc_desc_LN: Optional[bool] = False
    asc_desc_UC: Optional[bool] = False
    sorted: bool = False
