from typing import Optional

from pydantic import BaseModel, field_validator
from datetime import date, datetime, time

class AgentSchema(BaseModel):
    Date: date
    Agent: str
    Agent_Id: str
    Login_Time: datetime
    Logout_Time: datetime
    Duration: str

    @field_validator("Agent")
    def agent_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Agent name cannot be empty")
        return v

class BreakDataSchema(BaseModel):
    StartTime: datetime
    EndTime: datetime
    Agent: str
    AgentId: str
    Status: str
    StatusCodeItem: Optional[str] = None
    StatusCodeList: Optional[str] = None
    GroupName: Optional[str] = None
    TimeValue: Optional[str] = None
    TimePercentage: Optional[float] = None
    LoggedInTime: Optional[str] = None

    @field_validator("Agent")
    def agent_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Break Data cannot be empty")
        return v

class TimeOnStatusSchema(BaseModel):
    StartTime: date
    EndTime: date
    Agent: str
    AgentId: str
    AvailableTime: Optional[time] = None
    AvailableTimePercent: Optional[float] = None
    HandlingTime: Optional[time] = None
    HandlingTimePercent: Optional[float] = None
    WrapUpTime: Optional[time] = None
    WrapUpTimePercent: Optional[float] = None
    WorkingOfflineTime: Optional[time] = None
    WorkingOfflineTimePercent: Optional[float] = None
    OfferingTime: Optional[time] = None
    OfferingTimePercent: Optional[float] = None
    OnBreakTime: Optional[time] = None
    OnBreakTimePercent: Optional[float] = None
    BusyTime: Optional[time] = None
    BusyTimePercent: Optional[float] = None
    LoggedInTime: Optional[time] = None

    @field_validator("Agent")
    def agent_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Agent cannot be empty")
        return v

    @field_validator("AgentId")
    def agent_id_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("AgentId cannot be empty")
        return v

    @field_validator("AvailableTimePercent", "HandlingTimePercent", "WrapUpTimePercent", 
                    "WorkingOfflineTimePercent", "OfferingTimePercent", "OnBreakTimePercent",
                    "BusyTimePercent")
    def percent_range(cls, v):
        if v is not None and (v < 0 or v > 100):
            raise ValueError("Percent must be between 0 and 100")
        return v

