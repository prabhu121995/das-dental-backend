from typing import Optional,List
from pydantic import BaseModel, field_validator
from datetime import date, datetime, time


class UpdateUser(BaseModel):
    user_id: int
    username: Optional[str] = None
    password_hash: Optional[str] = None
    role_id: Optional[int] = None
    team_id: Optional[int] = None
    is_active: Optional[bool] = None
    agent_name: Optional[str] = None

class UserCreate(BaseModel):
    username: str
    password_hash: str
    role_id: int
    team_id: int
    is_active: bool = True
    agent_name: Optional[str] = None

class VonageUpdate(BaseModel):
    id: int
    name: Optional[str] = None
    eight_ID: Optional[str] = None
    Edgemd_ID: Optional[str] = None
    Modmed_ID: Optional[str] = None
    Team_ID: Optional[int] = 0
    updatedBy: Optional[int] = 0
    IsActive: Optional[int] = 0

class VonageCreate(BaseModel):
    name: str
    eight_ID: Optional[str] = None
    Edgemd_ID: Optional[str] = None
    Modmed_ID: Optional[str] = None
    Team_ID: Optional[int] = 0
    createdBy: Optional[int] = 0

# Team Models
class TeamBase(BaseModel):
    name: str

class TeamCreate(TeamBase):
    pass

class TeamUpdate(TeamBase):
    id: int
    name: str
    

class TeamResponse(TeamBase):
    id: int
    
    class Config:
        from_attributes = True


class ReportRequest(BaseModel):
    start_date: date
    end_date: date
    page: int = 1
    page_size: int = 100

class TransactionResponse(BaseModel):

    Id: int
    TimeFinished: datetime

    TransactionID: Optional[str]
    OriginalTransactionID: Optional[str]

    MediaType: Optional[str]
    CreationTime: Optional[datetime]

    Direction: Optional[str]
    Type: Optional[str]

    ChannelID: Optional[str]
    QueueName: Optional[str]

    Origination: Optional[str]
    Destination: Optional[str]

    CustomerName: Optional[str]
    CaseNumber: Optional[str]

    OutboundPhoneShortCode: Optional[str]
    OutboundPhoneCodeText: Optional[str]

    Participant: Optional[str]

    OfferActionTime: Optional[datetime]

    HandlingDuration: Optional[str]
    WrapUpDuration: Optional[str]
    ProcessingDuration: Optional[str]

    TimetoAbandon: Optional[str]

    RecordingFilenames: Optional[str]

    IVRTreatmentDuration: Optional[str]

    Hold: Optional[str]
    HoldDuration: Optional[str]

    WrapUpCodeListID: Optional[str]
    WrapUpCodeText: Optional[str]

    createdDate: Optional[date]

    agent_name: Optional[str]

class RefusedResponse(BaseModel):

    Id: int
    StartTime: date
    EndTime: Optional[date]

    Agent: Optional[str]
    AgentId: Optional[int]

    Accepted: Optional[int]
    Rejected: Optional[int]
    Presented: Optional[int]

    AcceptedPercent: Optional[float]
    RejectedPercent: Optional[float]

    AverageHandlingTime: Optional[str]
    AverageWrapUpTime: Optional[str]
    AverageBusyTime: Optional[str]

    CreatedDate: Optional[date]
    agent_name: Optional[str]

class NextechResponse(BaseModel):

    Id: int

    InputDate: Optional[date]

    CreatedbyLogin: Optional[str]
    PatientName: Optional[str]

    ApptDate: Optional[date]
    StartTime: Optional[str]

    Purpose: Optional[str]
    WebSite: Optional[str]

    Location: Optional[str]

    user_name: Optional[str]

    CreatedDate: Optional[datetime]

    agent_name: Optional[str]


class ModmedResponse(BaseModel):

    Id: int
    PatientName: Optional[str]

    PatientDOB: Optional[date]
    PatientPreferredPhone: Optional[str]

    AppointmentCreatedDate: Optional[date]
    AppointmentCreatedBy: Optional[str]

    Location: Optional[str]
    AppointmentType: Optional[str]

    AppointmentDate: Optional[date]
    AppointmentTime: Optional[str]

    AppointmentStatus: Optional[str]
    AppointmentRescheduled: Optional[str]

    AppointmentCount: Optional[int]

    PrimaryProvider: Optional[str]

    CreatedDate: Optional[datetime]
    agent_name: Optional[str]

class FSSCResponse(BaseModel):

    Id: int
    rec_id: Optional[str]

    Date: date
    Location: Optional[str]
    Form: Optional[str]

    SourceURL: Optional[str]
    Status: Optional[str]
    Reason: Optional[str]

    FirstTouchDate: Optional[datetime]
    FirstTouchUser: Optional[str]

    TimetoFirstTouchmins: Optional[int]

    LastTouchDate: Optional[datetime]
    LastTouchUser: Optional[str]

    CreatedDate: Optional[datetime]
    agent_name: Optional[str]

class AgentTimeOnStatusResponse(BaseModel):

    Id: int
    StartTime: datetime
    EndTime: Optional[datetime]
    Agent: str
    AgentId: int

    AvailableTime: Optional[str]
    AvailableTimePercent: Optional[float]

    HandlingTime: Optional[str]
    HandlingTimePercent: Optional[float]

    WrapUpTime: Optional[str]
    WrapUpTimePercent: Optional[float]

    WorkingOfflineTime: Optional[str]
    WorkingOfflineTimePercent: Optional[float]

    OfferingTime: Optional[str]
    OfferingTimePercent: Optional[float]

    OnBreakTime: Optional[str]
    OnBreakTimePercent: Optional[float]

    BusyTime: Optional[str]
    BusyTimePercent: Optional[float]

    LoggedInTime: Optional[str]

    CreatedDate: Optional[datetime]
    agent_name: Optional[str]

    notes: Optional[str]
    updatedby: Optional[str]
    updated_at: Optional[datetime]


class BreakDataResponse(BaseModel):
    Id: int
    StartTime: datetime
    EndTime: Optional[datetime]
    Agent: str
    AgentId: int
    Status: str
    StatusCodeItem: Optional[str]
    StatusCodeList: Optional[str]
    GroupName: Optional[str]
    TimeValue: Optional[str]
    TimePercentage: Optional[float]
    LoggedInTime: Optional[str]
    CreatedAt: Optional[datetime]
    agent_name: Optional[str] = None
    notes: Optional[str]
    updatedby: Optional[str]
    updated_at: Optional[datetime]

class AgentLoginResponse(BaseModel):
    id: int
    shiftdate: date
    agent: str
    agent_id: int
    login_time: Optional[datetime]
    logout_time: Optional[datetime]
    duration: Optional[str]
    CreatedAt: Optional[datetime]
    agent_name: Optional[str] = None
    notes: Optional[str]
    updatedby: Optional[int]
    updated_at: Optional[datetime]


class DeleteReportRequest(BaseModel):
    shiftdate: date
    reports: List[str]

class UpdateLoginRequest(BaseModel):
    id: int
    Login_Time: datetime
    Logout_Time: datetime
    Duration: str
    Notes: str
    updated_by: Optional[int] = None 

    @field_validator("id")
    def id_not_empty(cls, v):
        if not v:
            raise ValueError("ID cannot be empty")
        return v

class AgentSchema(BaseModel):
    Date: date
    Agent: str
    Agent_Id: str
    Login_Time: datetime
    Logout_Time: datetime
    Duration: str
    user_id: Optional[int] = None

    @field_validator("Agent")
    def agent_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Agent name cannot be empty")
        return v


class UpdateBreakDataSchema(BaseModel):
    id: int
    StartTime: datetime
    EndTime: datetime
    Status: str
    StatusCodeItem: Optional[str] = None
    StatusCodeList: Optional[str] = None
    TimeValue: Optional[str] = None
    TimePercentage: Optional[float] = None
    LoggedInTime: Optional[str] = None
    Notes: str
    updated_by: Optional[int] = None

    @field_validator("id")
    def id_not_empty(cls, v):
        if not v:
            raise ValueError("ID cannot be empty")
        return v
    @field_validator("Notes")
    def notes_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Notes cannot be empty")
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
    user_id: Optional[int] = None

    @field_validator("Agent")
    def agent_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Break Data cannot be empty")
        return v


class UpdateAgentTimeOnStatusRequest(BaseModel):

    Id: int
    StartTime: datetime
    EndTime: datetime
    AvailableTime: str
    AvailableTimePercent: str
    HandlingTime: str
    HandlingTimePercent: str
    WrapUpTime: str
    WrapUpTimePercent: str
    WorkingOfflineTime: str
    WorkingOfflineTimePercent: str
    OfferingTime: str
    OfferingTimePercent: str
    OnBreakTime: str
    OnBreakTimePercent: str
    BusyTime: str
    BusyTimePercent: str
    LoggedInTime: str
    Notes: str 
    updated_by: Optional[int] = None

    @field_validator("Notes")
    def notes_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Notes cannot be empty")
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
    user_id: Optional[int] = None

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
    user_id: Optional[int] = None

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
    user_id: Optional[int] = None

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
    user_id: Optional[int] = None

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
    user_id: Optional[int] = None

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
    user_id: Optional[int] = None

    @field_validator("PatientName")
    def patient_name_not_empty(cls, v):
        if v is not None and not str(v).strip():
            raise ValueError("Patient name cannot be empty")
        return v

class LoginRequest(BaseModel):
    username: str
    password: str