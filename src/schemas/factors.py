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
class ModifyAccountingFactorIn:
    MarketerID: str
    Period: str = str(current_year) + f"{current_month:02}"
    FactorID: str = None
    Plan: str = None
    TaxDeduction: int = Query(None, alias="TaxDeduction")
    TaxCoefficient: float = None
    CollateralDeduction: int = Query(None, alias="CollateralDeduction")
    CollateralCoefficient: float = None
    InsuranceDeduction: int = Query(None, alias="InsuranceDeduction")
    InsuranceCoefficient: float = None
    MarketerTotalIncome: int = Query(None, alias="MarketerTotalIncome")
    Payment: int = None
    Status: int = Query(None, alias="Status")
    # ContractID: str = None
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
class ModifyBaseFactorIn:
    MarketerID: str
    Period: str = str(current_year) + f"{current_month:02}"
    FactorID: str = None
    TotalTurnOver: int = Query(None, alias="TotalTurnOver")#TotalPureVolume
    TotalBrokerCommission: int = Query(None, alias="TotalBrokerCommission")#TotalFee
    TotalCMD: int = None
    TotalNetBrokerCommission: int = Query(None, alias="TotalNetBrokerCommission")#PureFee
    MarketerCommissionIncome: int = Query(None, alias="MarketerCommissionIncome")#MarketerFee
    FollowersIncome: int = None
    IsCmdConcluded: bool = False
    MaketerCMDIncome: int = None
    CreateDateTime: str = None
    UpdateDateTime: str = None


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
    timeGenerated: str
    error: str = Query("nothing")


@dataclass
class SearchFactorIn:

    MarketerID: str = Query(None)
    Period: Optional[str] = str(current_year) + f"{current_month:02}"
    FactorStatus: int = Query(None, alias="Status")
    FactorID: str = None
    ContractID: str = None
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")




@dataclass
class DeleteFactorIn:

    MarketerID: str = None
    Period: Optional[str] = str(current_year) + f"{current_month:02}"
    ID: str = None
    ContractID: str = None


@dataclass
class CalFactorIn:

    MarketerID: str = Query(None)
    Period: Optional[str] = str(current_year) + f"{current_month:02}"
    # Collateral: int = 0
    Additions: int = 0
    Deductions: int = 0


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
