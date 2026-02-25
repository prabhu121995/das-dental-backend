from typing import Optional

from pydantic import BaseModel, field_validator
from datetime import date, datetime

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