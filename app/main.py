from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Depends,APIRouter

from app.schemas import UpdateAgentTimeOnStatusRequest, UpdateBreakDataSchema, UpdateLoginRequest
from .services import process_excel_logindata, process_excel_daily_breakdata, process_excel_refused, process_excel_time_on_status, process_excel_transaction_data,process_excel_form_submission_data,process_excel_modmed_data,process_excel_nextch_data, process_update_break_data, process_update_login_data, process_update_time_on_status
from .dependencies import get_db
from .auth_service import login_user
from .jwt_handler import create_token, require_role
import shutil
import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
load_dotenv()


router = APIRouter()
app = FastAPI(
    title="DAS Agent Analytics API",
    description="DAS API description",
    version="2.1.0",
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


@app.post("/upload-excel-loginData/", tags=["DAS Module"])
async def upload_login_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    user_id = user["user_id"]
    # Call your existing sync function
    result = process_excel_logindata(temp_path, conn, user_id)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-daily-breakData/", tags=["DAS Module"])
async def upload_excel_daily_breakdata(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    user_id = user["user_id"]
    result = process_excel_daily_breakdata(temp_path, conn, user_id )

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-timeonstatus/", tags=["DAS Module"])
async def upload_excel_time_on_status(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    user_id = user["user_id"]
    result = process_excel_time_on_status(temp_path, conn, user_id)

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-transaction_data/", tags=["DAS Module"])
async def upload_transaction_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    user_id = user["user_id"]
    # Call your existing processing function
    result = process_excel_transaction_data(temp_path, conn, user_id)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-form-submissions/", tags=["DAS Module"])
async def upload_form_submission_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    user_id = user["user_id"]
    # Call your existing processing function
    result = process_excel_form_submission_data(temp_path, conn, user_id)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-modmed/", tags=["DAS Module"])
async def upload_modmed_data(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
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
    
    # Clean up
    os.remove(temp_path1)
    os.remove(temp_path2)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-nextech/", tags=["DAS Module"])
async def upload_nextech_data(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    user_id = user["user_id"]
    # Call your existing processing function
    result = process_excel_nextch_data(temp_path, conn, user_id)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-refused/", tags=["DAS Module"])
async def upload_refused_data(
    file: UploadFile = File(...),
    conn = Depends(get_db), user = Depends(require_role(["Admin","TeamLeader"]))
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    user_id = user["user_id"]
    # Call your existing processing function
    result = process_excel_refused(temp_path, conn, user_id)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.put("/update-login-time", tags=["DAS Update Module"],)
def update_login_time(
    data: UpdateLoginRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):
    user_id = user["user_id"]
    result = process_update_login_data(data, conn, user_id)
    return {
        "status": "Completed",
        "data": result
    }


@app.put("/update-break-data", tags=["DAS Update Module"])
async def update_break_data(
    data: UpdateBreakDataSchema,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = process_update_break_data(data, conn, user["user_id"])

    return {
        "status": "Completed",
        "data": result
    }

@app.put("/update-time-on-status", tags=["DAS Update Module"])
async def update_time_on_status(
    data: UpdateAgentTimeOnStatusRequest,
    conn = Depends(get_db),
    user = Depends(require_role(["Admin","TeamLeader"]))
):

    result = process_update_time_on_status(data, conn, user["user_id"])

    return {
        "status": "Completed",
        "data": result
    }

# ✅ include router AFTER routes
app.include_router(
    router,
    prefix="/auth",
    tags=["Authentication"]
)