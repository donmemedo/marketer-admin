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

