import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def create_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=das_db;"
        "UID=admin;"
        "PWD=admin@123;"
    )

