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
)


@dataclass
class MarketerIn:
    IdpID: str = None


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
    Period: str = str(current_year) + f"{current_month:02}"
    TotalPureVolume: int = Query(None, alias="TotalTurnOver")
    TotalFee: int = Query(None, alias="TotalBrokerCommission")
    PureFee: int = Query(None, alias="TotalNetBrokerCommission")
    MarketerFee: int = Query(None, alias="MarketerCommissionIncome")
    Plan: str = None
    Tax: int = Query(None, alias="TaxDeduction")
    TaxCoefficient: float = None
    Collateral: int = Query(None, alias="CollateralDeduction")
    CollateralCoefficient: float = None
    Insurance: int = Query(None, alias="InsuranceDeduction")
    InsuranceCoefficient: float = None
    FinalFee: int = Query(None, alias="MarketerTotalIncome")
    Payment: int = None
    FactorStatus: int = Query(None, alias="Status")
    ID: str = None
    ContractID: str = None
    CalculationCoefficient: float = None
    TotalCMD: int = None
    IsCmdConcluded: bool = False
    MaketerCMDIncome: int = None
    ReturnDuration: int = None
    InterimAmountDeduction: int = None
    EmployeeSalaryDeduction: int = None
    EmployerInsuranceDeduction: int = None
    RedemptionDeduction: int = None
    OtherDeduction: int = None
    OtherDeductionDescription: str = None
    CmdPayment: int = None
    CollateralReturnPayment: int = None
    InsuranceReturnPayment: int = None
    OtherPayment: int = None
    OtherPaymentDescription: str = None
    CreateDateTime: str = None
    UpdateDateTime: str = None


@dataclass
class ModifyFactorIN:
    MarketerID: str
    Period: str = str(current_year) + f"{current_month:02}"
    TotalPureVolume: int = None
    TotalFee: int = None
    PureFee: int = None
    MarketerFee: int = None
    TotalFeeOfFollowers: int = None
    CollateralOfThisMonth: int = None
    SumOfDeductions: int = None
    Payment: int = None
    FactorStatus: int = Query(None, alias="Status")


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
class SearchFactorIn:

    MarketerID: str = Query("")
    Period: Optional[str] = str(current_year) + f"{current_month:02}"
    FactorStatus: int = Query(None, alias="Status")
    ID: str = None
    ContractID: str = None


@dataclass
class DeleteFactorIn:

    MarketerID: str = None
    Period: Optional[str] = str(current_year) + f"{current_month:02}"
    ID: str = None
    ContractID: str = None


@dataclass
class CalFactorIn:

    MarketerID: str = Query("")
    Period: Optional[str] = str(current_year) + f"{current_month:02}"


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


@dataclass
class FactorsListIn(Pages):
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")
