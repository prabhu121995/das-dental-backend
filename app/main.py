from dotenv import load_dotenv
from typing import List
from fastapi import FastAPI, Form, UploadFile, File, Depends,APIRouter
from app.response import api_response
from app.schemas import  AgentLoginResponse, AgentTimeOnStatusResponse, AgentTimeOnStatusResponse, BreakDataResponse, DeleteReportRequest, FSSCResponse, ReportRequest,  UpdateAgentTimeOnStatusRequest, UpdateBreakDataSchema, UpdateLoginRequest
from .services import  get_agent_login_by_date, get_break_data_by_date_range, get_fssc_data_by_date_range, get_modmed_data, get_nextech_data, get_refused_data,  get_time_on_status_by_date_range, get_transaction_data, process_delete_reports, process_excel_logindata, process_excel_daily_breakdata, process_excel_refused, process_excel_time_on_status, process_excel_transaction_data,process_excel_form_submission_data,process_excel_modmed_data,process_excel_nextch_data, process_update_break_data, process_update_login_data, process_update_time_on_status
from .dependencies import get_db
from .auth_service import login_user
from .jwt_handler import create_token, require_role
from datetime import date
import shutil
import os
import warnings
import uuid
from fastapi.encoders import jsonable_encoder
warnings.filterwarnings("ignore", category=UserWarning)
load_dotenv()

router = APIRouter()
app = FastAPI(
    title="DAS Agent Analytics API",
    description="DAS API description",
    version="3.1.0",
    docs_url="/swagger",
    redoc_url="/redoc"
)


@router.post("/login")
def login(data: dict):

    user = login_user(data["username"], data["password"])

    if not user:
        return {"error": "Invalid credentials"}

    token = create_token(user)

    return {
        "access_token": token,
        "role": user["role"]
    }


@app.post(
    "/get-transaction-data",
    tags=["DAS Get Module"]
)
async def get_transaction_data_api(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_transaction_data(data, conn)

    return api_response(
        status="success",
        message="Transaction report fetched successfully",
        data=jsonable_encoder(result),
        status_code=200
    )

@app.post(
    "/get-refused-data",
    tags=["DAS Get Module"]
)
async def get_refused_data_api(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_refused_data(data, conn)

    return api_response(
        status="success",
        message="Refused report fetched successfully",
        data=jsonable_encoder(result),
        status_code=200
    )

@app.post(
    "/get-nextech-data",
    tags=["DAS Get Module"]
)
async def get_nextech_data_api(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_nextech_data(data, conn)

    return api_response(
        status="success",
        message="Nextech data fetched successfully",
        data=jsonable_encoder(result),
        status_code=200
    )

@app.post(
    "/get-agent-login",
    response_model=List[AgentLoginResponse],
    tags=["DAS Get Module"]
)
async def get_agent_login(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_agent_login_by_date(data, conn)

    return api_response(
        status="success",
        message="Agent login data fetched successfully",
        data=result,
        status_code=200
    )


@app.post(
    "/get-modmed-data",
    tags=["DAS Get Module"]
)
async def get_modmed_data_api(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_modmed_data(data, conn)

    return api_response(
        status="success",
        message="Modmed data fetched successfully",
        data=jsonable_encoder(result),
        status_code=200
    )

@app.post(
    "/get-break-data",
    response_model=List[BreakDataResponse],
    tags=["DAS Get Module"]
)
async def get_break_data(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_break_data_by_date_range(data, conn)

    return api_response(
        status="success",
        message="Break data fetched successfully",
        data=result,
        status_code=200
    )

@app.post(
    "/get-time-on-status",
    response_model=List[AgentTimeOnStatusResponse],
    tags=["DAS Get Module"]
)
async def get_time_on_status(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_time_on_status_by_date_range(data, conn)

    return api_response(
        status="success",
        message="Agent Time On Status data fetched successfully",
        data=result,
        status_code=200
    )

@app.post(
    "/get-submission-data",
    response_model=List[FSSCResponse],
    tags=["DAS Get Module"]
)
async def get_fssc_data(
    data: ReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = get_fssc_data_by_date_range(data, conn)

    return api_response(
        status="success",
        message="submission data fetched successfully",
        data=result,
        status_code=200
    )

@app.post("/upload-excel-loginData/", tags=["DAS Module"])
async def upload_login_data(
    file: UploadFile = File(...),
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    temp_path = f"temp_{uuid.uuid4()}_{file.filename}"

    try:
        # Save uploaded file
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        user_id = user["user_id"]

        # Process file
        result = process_excel_logindata(temp_path, conn, user_id)

        if result.get("error"):
            return api_response(
                status="failed",
                message="Error processing login data",
                data=result,
                status_code=500
            )

        return api_response(
            status="success",
            message="Login data uploaded successfully",
            data=result,
            status_code=200
        )

    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.post("/upload-excel-daily-breakData/", tags=["DAS Module"])
async def upload_excel_daily_breakdata(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):

    temp_path = f"temp_{file.filename}"

    try:

        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        user_id = user["user_id"]
        result = process_excel_daily_breakdata(temp_path, conn, user_id )

        if result.get("error"):
                return api_response(
                    status="failed",
                    message="Error processing daily break data",
                    data=result,
                    status_code=500
                )

        os.remove(temp_path)

        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.post("/upload-excel-timeonstatus/", tags=["DAS Module"])
async def upload_excel_time_on_status(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):

    temp_path = f"temp_{file.filename}"
    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        user_id = user["user_id"]
        result = process_excel_time_on_status(temp_path, conn, user_id)

        if result.get("error"):
                    return api_response(
                        status="failed",
                        message="Error processing time on status data",
                        data=result,
                        status_code=500
                    )

        os.remove(temp_path)

        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.post("/upload-excel-transaction_data/", tags=["DAS Module"])
async def upload_transaction_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        user_id = user["user_id"]
        # Call your existing processing function
        result = process_excel_transaction_data(temp_path, conn, user_id)  # This now calls the sync function
        
        if result.get("error"):
                    return api_response(
                        status="failed",
                        message="Error processing transaction data",
                        data=result,
                        status_code=500
                    )

        os.remove(temp_path)
        
        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.post("/upload-excel-form-submissions/", tags=["DAS Module"])
async def upload_form_submission_data(  # Changed endpoint name
    file: UploadFile = File(...),
    shiftdate: date = Form(...),# Need to work single date convert into date range in service layer
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        user_id = user["user_id"]

        data = {
        "shiftdate": shiftdate,
        "reports": ["submission"]
        }

        delete_result = process_delete_reports(data, conn)

        if delete_result.get("error"):
            return api_response(
                    status="failed",
                    message="Error deleting existing reports for the given date",
                    data=delete_result,
                    status_code=500
                )

        # Call your existing processing function
        result = process_excel_form_submission_data(temp_path, conn, user_id)  # This now calls the sync function
        
        if delete_result.get("error"):
            return api_response(
                    status="failed",
                    message="Error processing form submission data",
                    data=delete_result,
                    status_code=500
                )

        os.remove(temp_path)
        
        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.post("/upload-excel-modmed/", tags=["DAS Module"])
async def upload_modmed_data(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    try:
        # Save first file
        temp_path1 = f"temp_{file1.filename}"
        with open(temp_path1, "wb") as buffer:
            shutil.copyfileobj(file1.file, buffer)
        
        # Save second file
        temp_path2 = f"temp_{file2.filename}"
        with open(temp_path2, "wb") as buffer:
            shutil.copyfileobj(file2.file, buffer)
        
        user_id = user["user_id"]
        # Process both files together
        result = process_excel_modmed_data(temp_path1, temp_path2, conn, user_id)
        
        if result.get("error"):
            return api_response(
                    status="failed",
                    message="Error processing form modmed data",
                    data=result,
                    status_code=500
                )

        # Clean up
        os.remove(temp_path1)
        os.remove(temp_path2)
        
        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path1):
            os.remove(temp_path1)
        if os.path.exists(temp_path2):
            os.remove(temp_path2)

@app.post("/upload-excel-nextech/", tags=["DAS Module"])
async def upload_nextech_data(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        user_id = user["user_id"]
        # Call your existing processing function
        result = process_excel_nextch_data(temp_path, conn, user_id)  # This now calls the sync function
        
        if result.get("error"):
            return api_response(
                    status="failed",
                    message="Error processing form nextech data",
                    data=result,
                    status_code=500
                )

        os.remove(temp_path)
        
        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.post("/upload-excel-refused/", tags=["DAS Module"])
async def upload_refused_data(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    try:
        temp_path = f"temp_{file.filename}"
        
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        user_id = user["user_id"]
        # Call your existing processing function
        result = process_excel_refused(temp_path, conn, user_id)  # This now calls the sync function

        if result.get("error"):
            return api_response(
                    status="failed",
                    message="Error processing form refused data",
                    data=result,
                    status_code=500
                )
        os.remove(temp_path)
        
        return api_response(
                status="success",
                message="Reports uploaded successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

    finally:
        # Ensure file cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.put("/update-login-time", tags=["DAS Update Module"],)
def update_login_time(
    data: UpdateLoginRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):
    try:
        user_id = user["user_id"]
        result = process_update_login_data(data, conn, user_id)

        if result.get("error"):
                return api_response(
                        status="failed",
                        message="Error processing form update login data",
                        data=result,
                        status_code=500
                    )

        return api_response(
                status="success",
                message="Reports updated successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )


@app.put("/update-break-data", tags=["DAS Update Module"])
async def update_break_data(
    data: UpdateBreakDataSchema,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    try:
        result = process_update_break_data(data, conn, user["user_id"])

        if result.get("error"):
                return api_response(
                        status="failed",
                        message="Error processing form update break data",
                        data=result,
                        status_code=500
                    )    


        return api_response(
                status="success",
                message="Reports updated successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

@app.put("/update-time-on-status", tags=["DAS Update Module"])
async def update_time_on_status(
    data: UpdateAgentTimeOnStatusRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):
    try:
        result = process_update_time_on_status(data, conn, user["user_id"])

        if result.get("error"):
                    return api_response(
                            status="failed",
                            message="Error processing form update time on status data",
                            data=result,
                            status_code=500
                        )    

        return api_response(
                status="success",
                message="Reports updated successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

@app.delete("/delete-reports", tags=["DAS Delete Module"])
async def delete_reports(
    data: DeleteReportRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin"]))
):
    try:
        result =  process_delete_reports(data, conn)

        if result.get("error"):
                    return api_response(
                            status="failed",
                            message="Error processing form delete reports data",
                            data=result,
                            status_code=500
                        )    

        return api_response(
                status="success",
                message="Reports deleted successfully",
                data=result,
                status_code=200
            )
    except Exception as e:

        return api_response(
            status="failed",
            message="Unexpected server error",
            data=str(e),
            status_code=500
        )

# ✅ include router AFTER routes
app.include_router(
    router,
    prefix="/auth",
    tags=["Authentication"]
)