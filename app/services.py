import pandas as pd
import os
from pathlib import Path
import json
from .schemas import AgentSchema, BreakDataSchema

CHUNK_SIZE = 5000

def process_excel_LoginData(file_path, conn):

    df = pd.read_excel(file_path,skiprows=6)
    df.columns = df.columns.str.strip()


    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")


    # Convert time to timedelta
    df["Login Time"] = pd.to_timedelta(df["Login Time"])
    df["Logout Time"] = pd.to_timedelta(df["Logout Time"])

    # Combine date + time
    df["Login Time"] = df["Date"] + df["Login Time"]
    df["Logout Time"] = df["Date"] + df["Logout Time"]
    df["Login Time"] = df["Login Time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["Logout Time"] = df["Logout Time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df["Duration"] = pd.to_timedelta(df["Logged In Time"])
    df["Duration"] = pd.to_timedelta(df["Duration"]).astype(str).str[-8:]
    df = df.drop("Logged In Time", axis=1)

    total_rows = len(df)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = AgentSchema(
                    Date=row["Date"],
                    Agent=row["Agent"],
                    Agent_Id=row["Agent Id"],
                    Login_Time=row["Login Time"],
                    Logout_Time=row["Logout Time"],
                    Duration=str(row["Duration"])
                )

                validated_rows.append(tuple(validated.model_dump().values()))

            except Exception as e:
                total_failed += 1
                error_logs.append((
                    int(index),
                    "Validation",
                    str(e),
                    json.dumps(row.to_dict(), default=str)
                ))

        # Insert valid rows
        if validated_rows:
            try:
                cursor.executemany("""
                    EXEC insert_agent ?, ?, ?, ?, ?, ?
                """, validated_rows)

                total_inserted += len(validated_rows)

            except Exception as e:
                # If DB fails, log all rows as DB error
                for row_data in validated_rows:
                    error_logs.append((
                        None,
                        "Database",
                        str(e),
                        json.dumps(row_data, default=str)
                    ))

                total_failed += len(validated_rows)

        # Insert errors into error_logs table
        if error_logs:
            cursor.executemany("""
                    EXEC logs ?, ?, ?, ?
                """, error_logs)

        conn.commit()

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_DailyBreakData(file_path, conn):

    df_Break_data = pd.read_excel(file_path,skiprows=6)
    df_Break_data.columns = df_Break_data.columns.str.replace(" ", "")
    df_Break_data.rename(columns={"Time%": "TimePercentage","Group": "GroupName","Time":"TimeValue"}, inplace=True)
    df_Break_data.columns = df_Break_data.columns.str.strip()

    df_Break_data["StartTime"] = pd.to_datetime(df_Break_data["StartTime"], format="%m/%d/%Y %H:%M:%S")
    df_Break_data["EndTime"] = pd.to_datetime(df_Break_data["EndTime"], format="%m/%d/%Y %H:%M:%S")

    df_Break_data["TimeValue"] = pd.to_timedelta(df_Break_data["TimeValue"])
    df_Break_data["TimeValue"] = pd.to_timedelta(df_Break_data["TimeValue"]).astype(str).str[-8:]

    df_Break_data["LoggedInTime"] = pd.to_timedelta(df_Break_data["LoggedInTime"])
    df_Break_data["LoggedInTime"] = pd.to_timedelta(df_Break_data["LoggedInTime"]).astype(str).str[-8:]

    total_rows = len(df_Break_data)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_Break_data.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = BreakDataSchema(
                    StartTime=row["StartTime"],
                    EndTime=row["EndTime"],
                    Agent=row["Agent"],
                    AgentId=row["AgentId"],
                    Status=row["Status"],
                    StatusCodeItem=str(row["StatusCodeItem"]),
                    StatusCodeList=str(row["StatusCodeList"]),
                    GroupName=str(row["GroupName"]),
                    TimeValue=str(row["TimeValue"]),
                    TimePercentage=float(row["TimePercentage"]),
                    LoggedInTime=str(row["LoggedInTime"])
                    )

                validated_rows.append(tuple(validated.model_dump().values()))

            except Exception as e:
                total_failed += 1
                error_logs.append((
                    int(index),
                    "Validation",
                    str(e),
                    json.dumps(row.to_dict(), default=str)
                ))

        # Insert valid rows
        if validated_rows:
            try:
                cursor.executemany("""
                    EXEC sp_InsertAgentBreak_Data ?,?,?,?,?,?,?,?,?,?,?
                """, validated_rows)

                total_inserted += len(validated_rows)

            except Exception as e:
                # If DB fails, log all rows as DB error
                for row_data in validated_rows:
                    error_logs.append((
                        None,
                        "Database",
                        str(e),
                        json.dumps(row_data, default=str)
                    ))

                total_failed += len(validated_rows)

        # Insert errors into error_logs table
        if error_logs:
            cursor.executemany("""
                    EXEC logs ?, ?, ?, ?
                """, error_logs)

        conn.commit()

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }