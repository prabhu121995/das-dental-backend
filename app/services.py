import re

import numpy as np
import pandas as pd
import os
from pathlib import Path
import json
from .schemas import AgentSchema, BreakDataSchema, FSSCDataSchema, ModmedSchema, NextechSchema, TimeOnStatusSchema, transaction_schema,RefusedSchema


CHUNK_SIZE = 5000

def process_excel_logindata(file_path, conn):

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
                    Agent=str(row["Agent"]),
                    Agent_Id=str(row["Agent Id"]),
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

def process_excel_daily_breakdata(file_path, conn):

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

def process_excel_time_on_status(file_path, conn):

    df_Time_data = pd.read_excel(file_path,skiprows=6)
    df_Time_data.columns = df_Time_data.columns.str.replace(" ", "").str.replace("%", "Percent")
    time_columns = [
            "AvailableTime", "HandlingTime", "WrapUpTime", "WorkingOfflineTime",
            "OfferingTime", "OnBreakTime", "BusyTime","LoggedInTime"
        ]

    for col in time_columns:
        if col in df_Time_data.columns:
            df_Time_data[col] = pd.to_timedelta(df_Time_data[col])
            df_Time_data[col] = df_Time_data[col].astype(str).str[-8:]
            df_Time_data.columns = df_Time_data.columns.str.strip()

    

    total_rows = len(df_Time_data)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_Time_data.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = TimeOnStatusSchema(
                    StartTime=row["StartTime"],
                    EndTime=row["EndTime"],
                    Agent=row["Agent"],
                    AgentId=row["AgentId"],
                    AvailableTime=row["AvailableTime"],
                    AvailableTimePercent=row["AvailableTimePercent"],
                    HandlingTime=row["HandlingTime"],
                    HandlingTimePercent=row["HandlingTimePercent"],
                    WrapUpTime=row["WrapUpTime"],
                    WrapUpTimePercent=row["WrapUpTimePercent"],
                    WorkingOfflineTime=row["WorkingOfflineTime"],
                    WorkingOfflineTimePercent=row["WorkingOfflineTimePercent"],
                    OfferingTime=row["OfferingTime"],
                    OfferingTimePercent=row["OfferingTimePercent"],
                    OnBreakTime=row["OnBreakTime"],
                    OnBreakTimePercent=row["OnBreakTimePercent"],
                    BusyTime=row["BusyTime"],
                    BusyTimePercent=row["BusyTimePercent"],
                    LoggedInTime=row["LoggedInTime"]
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
                    EXEC sp_InsertAgentTimeOnStatus ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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

def process_excel_refused(file_path, conn):

    df_Refused_data = pd.read_excel(file_path,skiprows=6)
    df_Refused_data.columns = df_Refused_data.columns.str.replace(" ", "").str.replace("%", "Percent").str.replace("(", "").str.replace(")", "")
    df_Refused_data.columns = df_Refused_data.columns.str.strip()


    time_columns = ["AverageHandlingTime", "AverageWrapUpTime", "AverageBusyTime"]

    for col in time_columns:
            if col in df_Refused_data.columns:
                df_Refused_data[col] = pd.to_timedelta(df_Refused_data[col])
                df_Refused_data[col] = df_Refused_data[col].astype(str).str[-8:]
                df_Refused_data.columns = df_Refused_data.columns.str.strip()

    total_rows = len(df_Refused_data)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_Refused_data.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = RefusedSchema(
                    Date=row["Date"],
                    Agent=str(row["Agent"]),
                    AgentId=str(row["AgentId"]),
                    RefusedCount=int(row["RefusedCount"]),
                    AverageHandlingTime=str(row["AverageHandlingTime"]),
                    AverageHandlingTimePercent=float(row["AverageHandlingTimePercent"]),
                    AverageWrapUpTime=str(row["AverageWrapUpTime"]),
                    AverageWrapUpTimePercent=float(row["AverageWrapUpTimePercent"]),
                    AverageBusyTime=str(row["AverageBusyTime"]),
                    AverageBusyTimePercent=float(row["AverageBusyTimePercent"])
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
                    EXEC sp_InsertRefused ?,?,?,?,?,?,?,?,?,?,?,?
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

def process_excel_transaction_data(file_path, conn):

    df_trn_data = pd.read_excel(file_path)
    df_trn_data.columns = df_trn_data.columns.str.replace(" ", "").str.replace("%", "Percent").str.replace("(", "").str.replace(")", "")

    str_col = ["OfferActionTime", "TimeFinished", "CreationTime"]
    for col in str_col:
        if col in df_trn_data.columns:
            df_trn_data[col] = pd.to_datetime(df_trn_data[col], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna('1900-01-01')


    str_col = ["OriginalTransactionID", "CaseNumber", "Hold"]
    for col in str_col:
        if col in df_trn_data.columns:
            df_trn_data[col] = df_trn_data[col].astype(str).replace('nan', '0')


    # Or convert to int64 explicitly
    df_trn_data['ChannelID'] = df_trn_data['ChannelID'].fillna(0).astype('int64')
    df_trn_data = df_trn_data.replace(np.nan, 0)

    time_columns = ["HandlingDuration", "WrapUpDuration", "ProcessingDuration","IVRTreatmentDuration","HoldDuration","TimetoAbandon"]

    for col in time_columns:
        if col in df_trn_data.columns:
            df_trn_data[col] = df_trn_data[col].astype(object).replace('NaT', pd.Timedelta(seconds=0))
            df_trn_data[col] = pd.to_timedelta(df_trn_data[col])
            df_trn_data[col] = df_trn_data[col].astype(str).str[-8:]

    df_trn_data = df_trn_data[
        df_trn_data['TimeFinished'].notna() & 
        (df_trn_data['TimeFinished'] != '') & 
        (df_trn_data['TimeFinished'] != 'Total')
    ]
    df_trn_data['Participant'] = df_trn_data['Participant'].str.split(',').str[0].str.strip()

    

    total_rows = len(df_trn_data)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_trn_data.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = transaction_schema(
                    TimeFinished=str(row["TimeFinished"]),
                    TransactionID=str(row["TransactionID"]),
                    OriginalTransactionID=str(row["OriginalTransactionID"]),
                    MediaType=str(row["MediaType"]),
                    CreationTime=str(row["CreationTime"]),
                    Direction=str(row["Direction"]),
                    Type=str(row["Type"]),
                    ChannelID=str(row["ChannelID"]),
                    QueueName=str(row["QueueName"]),
                    Origination=str(row["Origination"]),
                    Destination=str(row["Destination"]),
                    CustomerName=str(row["CustomerName"]),
                    CaseNumber=str(row["CaseNumber"]),
                    OutboundPhoneShortCode=str(row["OutboundPhoneShortCode"]),
                    OutboundPhoneCodeText=str(row["OutboundPhoneCodeText"]),
                    Participant=str(row["Participant"]),
                    OfferActionTime=str(row["OfferActionTime"]),
                    HandlingDuration=str(row["HandlingDuration"]),
                    WrapUpDuration=str(row["WrapUpDuration"]),
                    ProcessingDuration=str(row["ProcessingDuration"]),
                    TimetoAbandon=str(row["TimetoAbandon"]),
                    RecordingFilenames=str(row["RecordingFilenames"]),
                    IVRTreatmentDuration=str(row["IVRTreatmentDuration"]),
                    Hold=str(row["Hold"]),
                    HoldDuration=str(row["HoldDuration"]),
                    WrapUpCodeListID=str(row["WrapUpCodeListID"]),
                    WrapUpCodeText=str(row["WrapUpCodeText"])
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
                    EXEC sp_InsertTransactionData ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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

def process_excel_form_submission_data(file_path, conn):

    df_fssc_data = pd.read_excel(file_path)
    df_fssc_data.columns = df_fssc_data.columns.str.replace(" ", "").str.replace("%", "Percent").str.replace("(", "").str.replace(")", "")
    df_fssc_data.columns = df_fssc_data.columns.str.strip()
    df_fssc_data.rename(columns={'#': 'rec_id'}, inplace=True)

    str_col = ["Date", "FirstTouchDate", "LastTouchDate"]
    for col in str_col:
        if col in df_fssc_data.columns:
            df_fssc_data[col] = pd.to_datetime(df_fssc_data[col], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna('1900-01-01')

    

    df_updated_fssc_data =  df_fssc_data[['rec_id', 'Date', 'Location', 'Form','SourceURL', 'Status', 'Reason', 'FirstTouchDate', 'FirstTouchUser','TimetoFirstTouchmins', 'LastTouchDate', 'LastTouchUser']]
    # Convert to Int64 and fill NaN with 0 (inplace)
    df_updated_fssc_data['TimetoFirstTouchmins'] = pd.to_numeric(df_updated_fssc_data['TimetoFirstTouchmins'], errors='coerce').fillna(0).astype(int)

    total_rows = len(df_updated_fssc_data)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_updated_fssc_data.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = FSSCDataSchema(
                    rec_id=str(row["rec_id"]),
                    Date=row["Date"],
                    Location=str(row["Location"]),
                    Form=str(row["Form"]),
                    SourceURL=str(row["SourceURL"]),
                    Status=str(row["Status"]),
                    Reason=str(row["Reason"]),
                    FirstTouchDate=row["FirstTouchDate"],
                    FirstTouchUser=str(row["FirstTouchUser"]),
                    TimetoFirstTouchmins=int(row["TimetoFirstTouchmins"]) if not pd.isna(row["TimetoFirstTouchmins"]) else 0,
                    LastTouchDate=row["LastTouchDate"],
                    LastTouchUser=str(row["LastTouchUser"])
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
                    EXEC sp_InsertFSSCData ?,?,?,?,?,?,?,?,?,?,?,?
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

def process_excel_modmed_data(file_path1, file_path2, conn):

    df_dtrc_data = pd.read_excel(file_path1)
    df_florida_data = pd.read_excel(file_path2)
    
    # Append/concatenate the DataFrames
    df_combined = pd.concat([df_dtrc_data, df_florida_data], ignore_index=True)

    df_combined.columns = df_combined.columns.str.replace(" ", "").str.replace("%", "Percent").str.replace("(", "").str.replace(")", "").str.replace("?","")
    df_combined.columns = df_combined.columns.str.strip()

    str_col = ["PatientDOB", "AppointmentCreatedDate", "AppointmentDate"]
    for col in str_col:
        if col in df_combined.columns:
            df_combined[col] = pd.to_datetime(df_combined[col], errors="coerce").dt.strftime('%Y-%m-%d').fillna('1900-01-01')

    total_rows = len(df_combined)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_combined.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = ModmedSchema(
                    PatientName=str(row["PatientName"]),
                    PatientDOB=row["PatientDOB"],
                    PatientPreferredPhone=str(row["PatientPreferredPhone"]),
                    AppointmentCreatedDate=row["AppointmentCreatedDate"],
                    AppointmentCreatedBy=str(row["AppointmentCreatedBy"]),
                    Location=str(row["Location"]),
                    AppointmentType=str(row["AppointmentType"]),
                    AppointmentDate=row["AppointmentDate"],
                    AppointmentTime=str(row["AppointmentTime"]),
                    AppointmentStatus=str(row["AppointmentStatus"]),
                    AppointmentRescheduled=str(row["AppointmentRescheduled"]),
                    AppointmentCount=int(row["AppointmentCount"]) if not pd.isna(row["AppointmentCount"]) else 0,
                    PrimaryProvider=str(row["PrimaryProvider"])
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
                    EXEC sp_InsertModmed ?,?,?,?,?,?,?,?,?,?,?,?,?
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

def process_excel_nextch_data(file_path, conn):

    df_nxt_data = pd.read_excel(file_path,skiprows=9)
    
    df_nxt_data.columns = df_nxt_data.columns.str.replace(" ", "").str.replace("%", "Percent").str.replace("(", "").str.replace(")", "")
    df_nxt_data.columns = df_nxt_data.columns.str.strip()
    df_nxt_data = df_nxt_data.dropna(subset=['InputDate'])

    # Use apply instead
    df_nxt_data['user_name'] = df_nxt_data['InputDate'].astype(str).apply(
        lambda x: re.search(r'([A-Za-z\s]+)', x).group(1).strip() if re.search(r'([A-Za-z\s]+)', x) else ''
    )
    # Forward fill - empty cells get the value from above
    df_nxt_data['user_name'] = df_nxt_data['user_name'].replace('', np.nan).ffill()


    # Remove all letters (a-z, A-Z)
    df_nxt_data['InputDate'] = df_nxt_data['InputDate'].astype(str).str.replace(r'[A-Za-z]', '', regex=True).str.strip()
    df_nxt_data = df_nxt_data[df_nxt_data['InputDate'] != '']

    df_nxt_data['InputDate'] = pd.to_datetime(df_nxt_data['InputDate'], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna('1900-01-01')
    df_nxt_data['ApptDate'] = pd.to_datetime(df_nxt_data['ApptDate'], errors="coerce").dt.strftime('%Y-%m-%d').fillna('1900-01-01')

    total_rows = len(df_nxt_data)
    total_inserted = 0
    total_failed = 0

    cursor = conn.cursor()
    cursor.fast_executemany = True

    for start in range(0, total_rows, CHUNK_SIZE):

        chunk = df_nxt_data.iloc[start:start + CHUNK_SIZE]

        validated_rows = []
        error_logs = []

        for index, row in chunk.iterrows():

            try:
                validated = NextechSchema(
                    InputDate=row["InputDate"],
                    CreatedbyLogin=str(row["CreatedbyLogin"]) if not pd.isna(row["CreatedbyLogin"]) else None,
                    PatientName=str(row["PatientName"]) if not pd.isna(row["PatientName"]) else None,
                    ApptDate=row["ApptDate"],
                    StartTime=str(row["StartTime"]) if not pd.isna(row["StartTime"]) else None,
                    Purpose=str(row["Purpose"]) if not pd.isna(row["Purpose"]) else None,
                    WebSite=str(row["WebSite"]) if not pd.isna(row["WebSite"]) else None,
                    Location=str(row["Location"]) if not pd.isna(row["Location"]) else None,
                    user_name=str(row["user_name"]) if not pd.isna(row["user_name"]) else None
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
                    EXEC sp_InsertNextech ?,?,?,?,?,?,?,?,?
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