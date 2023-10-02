"""_summary_
"""
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Optional, Any, List, Dict
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
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")


@dataclass
class Pages:
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")


class SortField(str, Enum):
    REGISTRATION_DATE = "RegisterDate"
    TotalPureVolume = "TotalPureVolume"


class ContractTypeEnum(str, Enum):
    Agency = "Agency"
    Independent = "Independent"


class CalculationBaseTypeEnum(str, Enum):
    PlanOne = "PlanOne"
    PlanTwo = "PlanTwo"
    PlanThree = "PlanThree"
    NoPlan = "NoPlan"
    ConstOne = "ConstOne"
    ConstTwo = "ConstTwo"
    ConstThree = "ConstThree"
    ConstFour = "ConstFour"


class CoefficientBaseTypeEnum(str, Enum):
    Plan = "Plan"
    Const = "Const"


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1


@dataclass
class FactorsListIn(Pages):
    from_date: str = Query(default=current_date, alias="StartDate")
    to_date: str = Query(default=current_date, alias="EndDate")


@dataclass
class AddMarketerContractIn:
    MarketerID: str
    CoefficientBaseType: CoefficientBaseTypeEnum = Query(
        CoefficientBaseTypeEnum.Plan, alias="CoefficientBaseType"
    )
    CalculationBaseType: CalculationBaseTypeEnum = Query(
        CalculationBaseTypeEnum.NoPlan, alias="CalculationBaseType"
    )
    ContractType: ContractTypeEnum = Query(
        ContractTypeEnum.Independent, alias="ContractType"
    )
    ContractNumber: str = None
    Description: str = None
    EndDate: str = None
    StartDate: str = None


@dataclass
class ModifyMarketerContractIn:
    ContractID: str
    CoefficientBaseType: CoefficientBaseTypeEnum = Query(
        CoefficientBaseTypeEnum.Plan, alias="CoefficientBaseType"
    )
    CalculationBaseType: CalculationBaseTypeEnum = Query(
        CalculationBaseTypeEnum.NoPlan, alias="CalculationBaseType"
    )
    ContractType: ContractTypeEnum = Query(
        ContractTypeEnum.Independent, alias="ContractType"
    )
    ContractNumber: str = None
    Description: str = None
    EndDate: str = None
    StartDate: str = None
    Title: str = None


@dataclass
class SearchMarketerContractIn:
    MarketerID: str = Query(None)
    ContractID: str = None
    ContractNumber: str = None
    Description: str = None
    EndDate: str = None
    StartDate: str = None
    Title: str = Query(None)
    CoefficientBaseType: CoefficientBaseTypeEnum = Query(
        None, alias="CoefficientBaseType"
    )
    CalculationBaseType: CalculationBaseTypeEnum = Query(
        None, alias="CalculationBaseType"
    )
    ContractType: ContractTypeEnum = Query(None, alias="ContractType")
    size: int = Query(10, alias="PageSize")
    page: int = Query(1, alias="PageNumber")


@dataclass
class DelMarketerContractIn:
    ContractID: str
