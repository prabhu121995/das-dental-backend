from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Depends
from .services import process_excel_logindata, process_excel_daily_breakdata, process_excel_refused, process_excel_time_on_status, process_excel_transaction_data,process_excel_form_submission_data,process_excel_modmed_data,process_excel_nextch_data
from .dependencies import get_db
import shutil
import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
load_dotenv()

app = FastAPI(
    title="DAS Agent Analytics API",
    description="DAS API description",
    version="2.1.0",
    docs_url="/swagger",
    redoc_url="/redoc"
)



@app.post("/upload-excel-loginData/", tags=["DAS Module"])
async def upload_login_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db)
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Call your existing sync function
    result = process_excel_logindata(temp_path, conn)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-daily-breakData/", tags=["DAS Module"])
async def upload_excel_daily_breakdata(
    file: UploadFile = File(...),
    conn = Depends(get_db)
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_excel_daily_breakdata(temp_path, conn)

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-timeonstatus/", tags=["DAS Module"])
async def upload_excel_time_on_status(
    file: UploadFile = File(...),
    conn = Depends(get_db)
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_excel_time_on_status(temp_path, conn)

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-transaction_data/", tags=["DAS Module"])
async def upload_transaction_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db)
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Call your existing processing function
    result = process_excel_transaction_data(temp_path, conn)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-form-submissions/", tags=["DAS Module"])
async def upload_form_submission_data(  # Changed endpoint name
    file: UploadFile = File(...),
    conn = Depends(get_db)
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Call your existing processing function
    result = process_excel_form_submission_data(temp_path, conn)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-modmed/", tags=["DAS Module"])
async def upload_modmed_data(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    conn = Depends(get_db)
):
    # Save first file
    temp_path1 = f"temp_{file1.filename}"
    with open(temp_path1, "wb") as buffer:
        shutil.copyfileobj(file1.file, buffer)
    
    # Save second file
    temp_path2 = f"temp_{file2.filename}"
    with open(temp_path2, "wb") as buffer:
        shutil.copyfileobj(file2.file, buffer)
    
    # Process both files together
    result = process_excel_modmed_data(temp_path1, temp_path2, conn)
    
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
    conn = Depends(get_db)
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Call your existing processing function
    result = process_excel_nextch_data(temp_path, conn)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-refused/", tags=["DAS Module"])
async def upload_refused_data(
    file: UploadFile = File(...),
    conn = Depends(get_db)
):
    temp_path = f"temp_{file.filename}"
    
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Call your existing processing function
    result = process_excel_refused(temp_path, conn)  # This now calls the sync function
    
    os.remove(temp_path)
    
    return {
        "status": "Completed",
        "data": result
    }