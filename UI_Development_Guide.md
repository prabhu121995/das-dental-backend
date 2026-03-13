# UI Development Guide for DAS Dental Backend

## Overview

This document provides a guide to create a user interface (UI) for the DAS Dental Analytics API. The backend is built with FastAPI and provides endpoints for agent analytics, transaction reports, and other dental-related data. The UI will allow users to interact with this data through a web interface.

## Purpose

The UI will serve as a dashboard for:

- Viewing transaction reports
- Analyzing agent performance
- Managing login and break data
- Generating filtered reports based on date ranges

## Technologies Recommended

- **Frontend Framework**: React.js (popular, component-based)
- **Styling**: CSS or Tailwind CSS
- **HTTP Client**: Axios or Fetch API for API calls
- **Charts**: Chart.js or D3.js for data visualization
- **State Management**: React hooks or Redux if needed

Alternative: If you prefer Python-based UI, consider Streamlit for rapid prototyping.

## Prerequisites

- Node.js and npm installed
- Basic knowledge of JavaScript/React
- Access to the running FastAPI backend (default: http://localhost:8000)

## Steps to Create the UI

### 1. Set Up the Project

```bash
npx create-react-app das-dental-ui
cd das-dental-ui
npm install axios chart.js react-chartjs-2
```

### 2. Project Structure

```
das-dental-ui/
├── public/
├── src/
│   ├── components/
│   │   ├── Login.js
│   │   ├── Dashboard.js
│   │   ├── ReportViewer.js
│   │   └── Charts.js
│   ├── App.js
│   ├── index.js
│   └── api.js
└── package.json
```

### 3. API Integration

Create `src/api.js` to handle API calls:

```javascript
import axios from "axios";

const API_BASE_URL = "http://localhost:8000";

export const login = (username, password) => {
  return axios.post(`${API_BASE_URL}/login`, { username, password });
};

export const getTransactionData = (startDate, endDate, page, pageSize) => {
  return axios.post(`${API_BASE_URL}/transaction-data`, {
    start_date: startDate,
    end_date: endDate,
    page,
    page_size: pageSize,
  });
};

// Add other API functions as needed
```

### 4. Authentication

Implement login component to authenticate users before accessing data.

### 5. Dashboard Components

- **Date Range Picker**: Allow users to select start and end dates
- **Data Tables**: Display transaction data in tabular format
- **Charts**: Visualize metrics like handling duration, processing time
- **Filters**: By channel, queue, customer, etc.

### 6. Key Features

- Responsive design for desktop and mobile
- Error handling for API calls
- Loading states
- Export functionality for reports

### 7. Deployment

- Build the app: `npm run build`
- Serve with a web server (e.g., Nginx) or deploy to platforms like Vercel/Netlify

## API Endpoints to Integrate

The DAS Dental Backend provides the following API endpoints. All data retrieval and modification endpoints require authentication via JWT token and appropriate user roles (Admin or TeamLeader for most operations).

### Authentication

- **POST /auth/login**
  - Description: Authenticate user and obtain JWT token
  - Body: `{ "username": string, "password": string }`
  - Response: `{ "access_token": string, "role": string }`
  - Roles: None (public)

### Data Retrieval Endpoints (GET Modules)

These endpoints retrieve analytics data based on date ranges. All require Admin/TeamLeader role.

- **POST /get-transaction-data**
  - Description: Retrieve transaction reports
  - Body: ReportRequest (start_date, end_date, page, page_size)
  - Response: Paginated transaction data

- **POST /get-refused-data**
  - Description: Retrieve refused call/transaction data
  - Body: ReportRequest
  - Response: Paginated refused data

- **POST /get-nextech-data**
  - Description: Retrieve Nextech system data
  - Body: ReportRequest
  - Response: Paginated Nextech data

- **POST /get-agent-login**
  - Description: Retrieve agent login/logout data
  - Body: ReportRequest
  - Response: List of AgentLoginResponse

- **POST /get-modmed-data**
  - Description: Retrieve Modmed system data
  - Body: ReportRequest
  - Response: Paginated Modmed data

- **POST /get-break-data**
  - Description: Retrieve agent break data
  - Body: ReportRequest
  - Response: List of BreakDataResponse

- **POST /get-time-on-status**
  - Description: Retrieve agent time on status data
  - Body: ReportRequest
  - Response: List of AgentTimeOnStatusResponse

- **POST /get-submission-data**
  - Description: Retrieve form submission data
  - Body: ReportRequest
  - Response: List of FSSCResponse

### Data Upload Endpoints

These endpoints allow uploading Excel files for data processing. All require Admin/TeamLeader role.

- **POST /upload-excel-loginData/**
  - Description: Upload agent login data Excel file
  - Body: Multipart form with file

- **POST /upload-excel-daily-breakData/**
  - Description: Upload daily break data Excel file
  - Body: Multipart form with file

- **POST /upload-excel-timeonstatus/**
  - Description: Upload time on status data Excel file
  - Body: Multipart form with file

- **POST /upload-excel-transaction_data/**
  - Description: Upload transaction data Excel file
  - Body: Multipart form with file

- **POST /upload-excel-form-submissions/**
  - Description: Upload form submission data Excel file
  - Body: Multipart form with file, shiftdate (date)

- **POST /upload-excel-modmed/**
  - Description: Upload Modmed data (two Excel files)
  - Body: Multipart form with file1, file2

- **POST /upload-excel-nextech/**
  - Description: Upload Nextech data Excel file
  - Body: Multipart form with file

- **POST /upload-excel-refused/**
  - Description: Upload refused data Excel file
  - Body: Multipart form with file

### Data Update Endpoints

These endpoints allow updating existing data. All require Admin/TeamLeader role.

- **PUT /update-login-time**
  - Description: Update agent login time data
  - Body: UpdateLoginRequest

- **PUT /update-break-data**
  - Description: Update break data
  - Body: UpdateBreakDataSchema

- **PUT /update-time-on-status**
  - Description: Update time on status data
  - Body: UpdateAgentTimeOnStatusRequest

### Data Deletion Endpoints

- **DELETE /delete-reports**
  - Description: Delete reports by type and date
  - Body: DeleteReportRequest
  - Roles: Admin only

### Common Request/Response Patterns

- **ReportRequest Schema**: `{ start_date: date, end_date: date, page: int, page_size: int }`
- **Response Format**: `{ status: string, message: string, data: array, total_rows: int, status_code: int }`
- **Authentication**: Include `Authorization: Bearer <token>` header for protected endpoints
- **Pagination**: Most GET endpoints support pagination with page and page_size parameters

For detailed schemas and interactive testing, visit the Swagger UI at `/swagger` or ReDoc at `/redoc`.

## Data Schemas and Excel Column Specifications

To help UI developers understand the data structures, below are the expected columns for Excel uploads and the columns returned in API responses. This information is crucial for creating upload forms, data tables, and validation logic.

### Transaction Data

**Upload Columns (Excel file for /upload-excel-transaction_data/):**

- TimeFinished (datetime)
- TransactionID (string, required)
- OriginalTransactionID (string)
- MediaType (string)
- CreationTime (datetime)
- Direction (string)
- Type (string)
- ChannelID (string)
- QueueName (string)
- Origination (string)
- Destination (string)
- CustomerName (string)
- CaseNumber (string)
- OutboundPhoneShortCode (string)
- OutboundPhoneCodeText (string)
- Participant (string)
- OfferActionTime (datetime)
- HandlingDuration (string, default '00:00:00')
- WrapUpDuration (string, default '00:00:00')
- ProcessingDuration (string, default '00:00:00')
- TimetoAbandon (string, default '00:00:00')
- RecordingFilenames (string)
- IVRTreatmentDuration (string, default '00:00:00')
- Hold (string)
- HoldDuration (string, default '00:00:00')
- WrapUpCodeListID (string)
- WrapUpCodeText (string)

**View Columns (Response from /get-transaction-data):**

- Id (int)
- TimeFinished (datetime)
- TransactionID (string)
- OriginalTransactionID (string)
- MediaType (string)
- CreationTime (datetime)
- Direction (string)
- Type (string)
- ChannelID (string)
- QueueName (string)
- Origination (string)
- Destination (string)
- CustomerName (string)
- CaseNumber (string)
- OutboundPhoneShortCode (string)
- OutboundPhoneCodeText (string)
- Participant (string)
- OfferActionTime (datetime)
- HandlingDuration (string)
- WrapUpDuration (string)
- ProcessingDuration (string)
- TimetoAbandon (string)
- RecordingFilenames (string)
- IVRTreatmentDuration (string)
- Hold (string)
- HoldDuration (string)
- WrapUpCodeListID (string)
- WrapUpCodeText (string)
- createdDate (date)
- agent_name (string)

### Agent Login Data

**Upload Columns (Excel file for /upload-excel-loginData/):**

- Date (date)
- Agent (string, required)
- Agent_Id (string)
- Login_Time (datetime)
- Logout_Time (datetime)
- Duration (string)

**View Columns (Response from /get-agent-login):**

- id (int)
- shiftdate (date)
- agent (string)
- agent_id (int)
- login_time (datetime)
- logout_time (datetime)
- duration (string)
- CreatedAt (datetime)
- agent_name (string)
- notes (string)
- updatedby (int)
- updated_at (datetime)

### Break Data

**Upload Columns (Excel file for /upload-excel-daily-breakData/):**

- StartTime (datetime)
- EndTime (datetime)
- Agent (string, required)
- AgentId (string)
- Status (string)
- StatusCodeItem (string)
- StatusCodeList (string)
- GroupName (string)
- TimeValue (string)
- TimePercentage (float)
- LoggedInTime (string)

**View Columns (Response from /get-break-data):**

- Id (int)
- StartTime (datetime)
- EndTime (datetime)
- Agent (string)
- AgentId (int)
- Status (string)
- StatusCodeItem (string)
- StatusCodeList (string)
- GroupName (string)
- TimeValue (string)
- TimePercentage (float)
- LoggedInTime (string)
- CreatedAt (datetime)
- agent_name (string)
- notes (string)
- updatedby (string)
- updated_at (datetime)

### Time on Status Data

**Upload Columns (Excel file for /upload-excel-timeonstatus/):**

- StartTime (date)
- EndTime (date)
- Agent (string, required)
- AgentId (string, required)
- AvailableTime (time)
- AvailableTimePercent (float, 0-100)
- HandlingTime (time)
- HandlingTimePercent (float, 0-100)
- WrapUpTime (time)
- WrapUpTimePercent (float, 0-100)
- WorkingOfflineTime (time)
- WorkingOfflineTimePercent (float, 0-100)
- OfferingTime (time)
- OfferingTimePercent (float, 0-100)
- OnBreakTime (time)
- OnBreakTimePercent (float, 0-100)
- BusyTime (time)
- BusyTimePercent (float, 0-100)
- LoggedInTime (time)

**View Columns (Response from /get-time-on-status):**

- Id (int)
- StartTime (datetime)
- EndTime (datetime)
- Agent (string)
- AgentId (int)
- AvailableTime (string)
- AvailableTimePercent (float)
- HandlingTime (string)
- HandlingTimePercent (float)
- WrapUpTime (string)
- WrapUpTimePercent (float)
- WorkingOfflineTime (string)
- WorkingOfflineTimePercent (float)
- OfferingTime (string)
- OfferingTimePercent (float)
- OnBreakTime (string)
- OnBreakTimePercent (float)
- BusyTime (string)
- BusyTimePercent (float)
- LoggedInTime (string)
- CreatedDate (datetime)
- agent_name (string)
- notes (string)
- updatedby (string)
- updated_at (datetime)

### Refused Data

**Upload Columns (Excel file for /upload-excel-refused/):**

- StartTime (datetime)
- EndTime (datetime)
- Agent (string)
- AgentId (string)
- Accepted (int, default 0)
- Rejected (int, default 0)
- Presented (int, default 0)
- AcceptedPercent (float, default 0)
- RejectedPercent (float, default 0)
- AverageHandlingTime (string)
- AverageWrapUpTime (string)
- AverageBusyTime (string)

**View Columns (Response from /get-refused-data):**

- Id (int)
- StartTime (date)
- EndTime (date)
- Agent (string)
- AgentId (int)
- Accepted (int)
- Rejected (int)
- Presented (int)
- AcceptedPercent (float)
- RejectedPercent (float)
- AverageHandlingTime (string)
- AverageWrapUpTime (string)
- AverageBusyTime (string)
- CreatedDate (date)
- agent_name (string)

### Form Submission Data (FSSC)

**Upload Columns (Excel file for /upload-excel-form-submissions/):**

- rec_id (string)
- Date (datetime)
- Location (string)
- Form (string)
- SourceURL (string)
- Status (string)
- Reason (string)
- FirstTouchDate (datetime)
- FirstTouchUser (string)
- TimetoFirstTouchmins (int, default 0)
- LastTouchDate (datetime)
- LastTouchUser (string)

**View Columns (Response from /get-submission-data):**

- Id (int)
- rec_id (string)
- Date (date)
- Location (string)
- Form (string)
- SourceURL (string)
- Status (string)
- Reason (string)
- FirstTouchDate (datetime)
- FirstTouchUser (string)
- TimetoFirstTouchmins (int)
- LastTouchDate (datetime)
- LastTouchUser (string)
- CreatedDate (datetime)
- agent_name (string)

### Modmed Data

**Upload Columns (Excel files for /upload-excel-modmed/ - two files):**

- PatientName (string)
- PatientDOB (date)
- PatientPreferredPhone (string)
- AppointmentCreatedDate (datetime)
- AppointmentCreatedBy (string)
- Location (string)
- AppointmentType (string)
- AppointmentDate (date)
- AppointmentTime (string)
- AppointmentStatus (string)
- AppointmentRescheduled (string)
- AppointmentCount (int, default 0)
- PrimaryProvider (string)

**View Columns (Response from /get-modmed-data):**

- Id (int)
- PatientName (string)
- PatientDOB (date)
- PatientPreferredPhone (string)
- AppointmentCreatedDate (date)
- AppointmentCreatedBy (string)
- Location (string)
- AppointmentType (string)
- AppointmentDate (date)
- AppointmentTime (string)
- AppointmentStatus (string)
- AppointmentRescheduled (string)
- AppointmentCount (int)
- PrimaryProvider (string)
- CreatedDate (datetime)
- agent_name (string)

### Nextech Data

**Upload Columns (Excel file for /upload-excel-nextech/):**

- InputDate (datetime)
- CreatedbyLogin (string)
- PatientName (string)
- ApptDate (datetime)
- StartTime (string)
- Purpose (string)
- WebSite (string)
- Location (string)
- user_name (string)

**View Columns (Response from /get-nextech-data):**

- Id (int)
- InputDate (date)
- CreatedbyLogin (string)
- PatientName (string)
- ApptDate (date)
- StartTime (string)
- Purpose (string)
- WebSite (string)
- Location (string)
- user_name (string)
- CreatedDate (datetime)
- agent_name (string)

### Notes for UI Developers

- **Required Fields**: Marked as "required" in upload schemas must be present and non-empty in Excel files
- **Data Types**: Pay attention to datetime vs date vs string formats
- **Validation**: Implement client-side validation matching the Pydantic validators
- **File Upload**: Use multipart/form-data for file uploads
- **Error Handling**: Check for validation errors in API responses
- **Pagination**: Use page and page_size for large datasets
- **Date Formats**: Use ISO format (YYYY-MM-DDTHH:MM:SS) for datetime fields
- **Percentages**: Values should be between 0-100 for percentage fields

## Security Considerations

- Use HTTPS in production
- Store JWT tokens securely (localStorage or secure cookies)
- Validate user inputs
- Implement logout functionality

## Next Steps

1. Review the FastAPI documentation at /redoc
2. Design wireframes for the UI
3. Implement components incrementally
4. Test with real data
5. Add unit tests

For more details, refer to the backend API documentation.</content>
<parameter name="filePath">c:\Prabhu\Project\DAS\das_app\das-dental-backend\UI_Development_Guide.md
