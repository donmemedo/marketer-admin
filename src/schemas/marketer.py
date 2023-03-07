from fastapi import Query
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Optional


@dataclass
class MarketersProfileIn:
    ...


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

