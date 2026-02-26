from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Depends
from .services import process_excel_DailyBreakData, process_excel_LoginData, process_excel_TimeOnStatus
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



@app.post("/upload-excel-LoginData/", tags=["DAS Module"])
async def upload_excel(
    file: UploadFile = File(...),
    conn = Depends(get_db)
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_excel_LoginData(temp_path, conn)

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-Daily-BreakData/", tags=["DAS Module"])
async def upload_excelDailyBreakData(
    file: UploadFile = File(...),
    conn = Depends(get_db)
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_excel_DailyBreakData(temp_path, conn)

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }

@app.post("/upload-excel-timeonstatus/", tags=["DAS Module"])
async def upload_excelTimeOnStatus(
    file: UploadFile = File(...),
    conn = Depends(get_db)
):

    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_excel_TimeOnStatus(temp_path, conn)

    os.remove(temp_path)

    return {
        "status": "Completed",
        "data": result
    }