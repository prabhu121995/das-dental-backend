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
class RefusedSchema(BaseModel):
    StartTime: Optional[datetime] = None
    EndTime: Optional[datetime] = None
    Agent: Optional[str] = None
    AgentId: Optional[str] = None
    Accepted: Optional[int] = 0
    Rejected: Optional[int] = 0
    Presented: Optional[int] = 0
    AcceptedPercent: Optional[float] = 0
    RejectedPercent: Optional[float] = 0
    AverageHandlingTime: Optional[str] = None
    AverageWrapUpTime: Optional[str] = None
    AverageBusyTime: Optional[str] = None

    @field_validator("Agent")
    def agent_not_empty(cls, v):
        if v is not None and not str(v).strip():
            raise ValueError("Agent name cannot be empty")
        return v
class transaction_schema(BaseModel):
    TimeFinished: Optional[datetime] = None
    TransactionID: str
    OriginalTransactionID: Optional[str] = None
    MediaType: Optional[str] = None
    CreationTime: Optional[datetime] = None
    Direction: Optional[str] = None
    Type: Optional[str] = None
    ChannelID: Optional[str] = None
    QueueName: Optional[str] = None
    Origination: Optional[str] = None
    Destination: Optional[str] = None
    CustomerName: Optional[str] = None
    CaseNumber: Optional[str] = None
    OutboundPhoneShortCode: Optional[str] = None
    OutboundPhoneCodeText: Optional[str] = None
    Participant: Optional[str] = None
    OfferActionTime: Optional[datetime] = None
    HandlingDuration: Optional[str] = '00:00:00'
    WrapUpDuration: Optional[str] = '00:00:00'
    ProcessingDuration: Optional[str] = '00:00:00'
    TimetoAbandon: Optional[str] = '00:00:00'
    RecordingFilenames: Optional[str] = None
    IVRTreatmentDuration: Optional[str] = '00:00:00'
    Hold: Optional[str] = None
    HoldDuration: Optional[str] = '00:00:00'
    WrapUpCodeListID: Optional[str] = None
    WrapUpCodeText: Optional[str] = None

    @field_validator("TransactionID")
    def transaction_not_empty(cls, v):
        if not v or not str(v).strip():
            raise ValueError("TransactionID cannot be empty")
        return v
class FSSCDataSchema(BaseModel):
    rec_id: Optional[str] = None
    Date: Optional[datetime] = None
    Location: Optional[str] = None
    Form: Optional[str] = None
    SourceURL: Optional[str] = None
    Status: Optional[str] = None
    Reason: Optional[str] = None
    FirstTouchDate: Optional[datetime] = None
    FirstTouchUser: Optional[str] = None
    TimetoFirstTouchmins: Optional[int] = 0
    LastTouchDate: Optional[datetime] = None
    LastTouchUser: Optional[str] = None

    @field_validator("rec_id")
    def rec_id_not_empty(cls, v):
        if v is not None and not str(v).strip():
            raise ValueError("rec_id cannot be empty")
        return v  
class ModmedSchema(BaseModel):
    PatientName: Optional[str] = None
    PatientDOB: Optional[date] = None
    PatientPreferredPhone: Optional[str] = None
    AppointmentCreatedDate: Optional[datetime] = None
    AppointmentCreatedBy: Optional[str] = None
    Location: Optional[str] = None
    AppointmentType: Optional[str] = None
    AppointmentDate: Optional[date] = None
    AppointmentTime: Optional[str] = None
    AppointmentStatus: Optional[str] = None
    AppointmentRescheduled: Optional[str] = None
    AppointmentCount: Optional[int] = 0
    PrimaryProvider: Optional[str] = None

    @field_validator("PatientName")
    def patient_name_not_empty(cls, v):
        if v is not None and not str(v).strip():
            raise ValueError("Patient name cannot be empty")
        return v

class NextechSchema(BaseModel):
    InputDate: Optional[datetime] = None
    CreatedbyLogin: Optional[str] = None
    PatientName: Optional[str] = None
    ApptDate: Optional[datetime] = None
    StartTime: Optional[str] = None
    Purpose: Optional[str] = None
    WebSite: Optional[str] = None
    Location: Optional[str] = None
    user_name: Optional[str] = None

    @field_validator("PatientName")
    def patient_name_not_empty(cls, v):
        if v is not None and not str(v).strip():
            raise ValueError("Patient name cannot be empty")
        return v