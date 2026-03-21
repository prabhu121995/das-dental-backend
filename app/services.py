import re
import calendar
import numpy as np
import pandas as pd
import os
from pathlib import Path
from typing import List, Optional, Tuple, Any
import json
from .schemas import AgentSchema, BreakDataSchema, FSSCDataSchema, GroupCreate, GroupUpdate, ModmedSchema, NextechSchema, PracticeCreate, PracticeGroupCreate, PracticeGroupUpdate, PracticeUpdate, TeamCreate, TeamUpdate, TimeOnStatusSchema, UpdateAgentTimeOnStatusRequest, UpdateBreakDataSchema, transaction_schema,RefusedSchema,UpdateLoginRequest


CHUNK_SIZE = 5000

REPORT_TABLE_MAP = {
    "login": ("agent_login", "shiftdate"),
    "break": ("Agent_Break_Data", "StartTime"),
    "status": ("AgentTimeOnStatus", "StartTime"),
    "refused": ("Refused", "StartTime"),
    "submission": ("FSSCData", "Date"),
    "transaction": ("TransactionData", "TimeFinished"),
    "modmed": ("Modmed", "AppointmentCreatedDate"),
    "nextech": ("Nextech", "InputDate")
}

def update_nextech_service(nextech_data, conn,updatedBy) -> bool:
    """
    Execute sp_Nextech_Update stored procedure
    """

    cursor = conn.cursor()

    try:
        cursor.execute("""
            EXEC sp_Nextech_Update
                @id=?,
                @name=?,
                @Nextech_ID=?,
                @Team_ID=?,
                @GroupID=?,
                @updatedBy=?,
                @IsActive=?
        """,
        nextech_data.id,
        nextech_data.name,
        nextech_data.Nextech_ID,
        nextech_data.Team_ID,
        nextech_data.GroupID,
        updatedBy,
        nextech_data.IsActive
        )

        conn.commit()

        return True

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()


def insert_nextech(nextech_data, conn, created_by) -> bool:
    """
    Execute sp_Nextech_Insert stored procedure
    Returns: True if inserted successfully
    """

    cursor = conn.cursor()

    try:
        cursor.execute("""
            EXEC sp_Nextech_Insert
                @name=?,
                @Nextech_ID=?,
                @Team_ID=?,
                @GroupID=?,
                @createdBy=?
        """,
        nextech_data.name,
        nextech_data.Nextech_ID,
        nextech_data.Team_ID,
        nextech_data.GroupID,
        created_by
        )

        cursor.commit()

        return cursor.rowcount > 0

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()

def db_get_NextTechID_data(conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC usp_next_tech_id"
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def db_get_practice_data(conn):
    cursor = conn.cursor()

    cursor.execute(
        "EXEC usp_get_practice"
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def db_create_practice(practice_data: PracticeCreate, conn) -> bool:
    """
    Execute sp_practice_create stored procedure
    Returns: True if created, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC sp_practice_create @PracticeName = ?,@Practice = ?",
            practice_data.PracticeName.strip(),practice_data.Practice.strip(),
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def db_update_practice(practice_data: PracticeUpdate, conn) -> bool:
    """
    Execute sp_practice_update stored procedure
    Returns: True if updated, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC sp_practice_update  @id = ?, @PracticeName = ?, @Practice = ?",
            practice_data.id, practice_data.PracticeName.strip(), practice_data.Practice.strip()
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def db_get_group_data(conn) -> Optional[dict]:
    cursor = conn.cursor()

    try:
        cursor.execute("EXEC usp_get_group")
        rows = cursor.fetchall()

        if not rows:
            return None

        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    finally:
        cursor.close()

def db_create_group(group_data: GroupCreate, conn) -> bool:
    """
    Execute sp_group_create stored procedure
    Returns: True if created, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC sp_group_create @Groups = ?",
            group_data.name.strip()
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def db_update_group(group_data: GroupUpdate, conn) -> bool:
    """
    Execute sp_group_update stored procedure
    Returns: True if updated, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC sp_group_update  @id = ?, @Groups = ?",
            group_data.id, group_data.name.strip()
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def db_get_practice_group_data(conn):
    cursor = conn.cursor()

    cursor.execute(
        "EXEC usp_get_practice_group"
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def db_create_practice_group(practice_data: PracticeGroupCreate, conn, created_by) -> bool:
    """
    Execute usp_insert_practice_group stored procedure
    Returns: True if created, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC usp_insert_practice_group @QueueName = ?,@Practice = ?,@Groups = ?,@createdBy=?",
            practice_data.QueueName.strip(),practice_data.Practice.strip(),practice_data.Groups.strip(),created_by
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def db_update_practice_group(practice_data: PracticeGroupUpdate, conn) -> bool:
    """
    Execute usp_update_practice_group stored procedure
    Returns: True if updated, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC usp_update_practice_group  @IntPracticeID = ?, @QueueName = ?, @Practice = ?, @Groups = ?",
            practice_data.IntPracticeID, practice_data.QueueName.strip(), practice_data.Practice.strip(), practice_data.Groups.strip()
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def update_user_service(user_data,created_by, conn) -> bool:
    """
    Execute sp_UpdateUser stored procedure
    """

    cursor = conn.cursor()

    try:
        cursor.execute("""
            EXEC sp_UpdateUser
                @user_id=?,
                @username=?,
                @password_hash=?,
                @role_id=?,
                @team_id=?,
                @is_active=?,
                @agent_name=?,
                @created_by=?
        """,
            user_data.user_id,
            user_data.username,
            user_data.password_hash,
            user_data.role_id,
            user_data.team_id,
            user_data.is_active,
            user_data.agent_name,
            created_by
        )

        conn.commit()

        return True

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()

def db_get_user_data(conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetAllUsers"
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def insert_user(user_details,user_id, conn) -> bool:
    """
    Execute sp_CreateUser stored procedure
    Returns: True if inserted successfully
    """

    cursor = conn.cursor()

    try:
        cursor.execute("""
            EXEC sp_CreateUser
                @username=?,
                @password_hash=?,
                @role_id=?,
                @team_id=?,
                @is_active=?,
                @agent_name=?,
                @created_by=?
        """,
            user_details.username,
            user_details.password_hash,
            user_details.role_id,
            user_details.team_id,
            user_details.is_active,
            user_details.agent_name,
            user_id
        )

        cursor.commit()

        return cursor.rowcount > 0

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()


def update_vonage_service(vonage_data, conn) -> bool:
    """
    Execute sp_VonageID_Update stored procedure
    """

    cursor = conn.cursor()

    try:
        cursor.execute("""
            EXEC sp_VonageID_Update
                @id=?,
                @name=?,
                @eight_ID=?,
                @Edgemd_ID=?,
                @Modmed_ID=?,
                @Team_ID=?,
                @updatedBy=?,
                @IsActive=?
        """,
        vonage_data.id,
        vonage_data.name,
        vonage_data.eight_ID,
        vonage_data.Edgemd_ID,
        vonage_data.Modmed_ID,
        vonage_data.Team_ID,
        vonage_data.updatedBy,
        vonage_data.IsActive
        )

        conn.commit()

        return True

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()


def insert_vonage(vonage_data, conn) -> bool:
    """
    Execute sp_VonageID_Insert stored procedure
    Returns: True if inserted successfully
    """

    cursor = conn.cursor()

    try:
        cursor.execute("""
            EXEC sp_VonageID_Insert
                @name=?,
                @eight_ID=?,
                @Edgemd_ID=?,
                @Modmed_ID=?,
                @Team_ID=?,
                @createdBy=?
        """,
        vonage_data.name,
        vonage_data.eight_ID,
        vonage_data.Edgemd_ID,
        vonage_data.Modmed_ID,
        vonage_data.Team_ID,
        vonage_data.createdBy
        )

        cursor.commit()

        return cursor.rowcount > 0

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()

def db_get_VonageID_data(conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_VonageID_GetAll"
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows


def db_get_all_teams(conn):
    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_team_get_all"
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def db_get_team_by_id(team_id: int, conn) -> Optional[dict]:
    cursor = conn.cursor()

    try:
        cursor.execute("EXEC sp_team_get_by_id @id = ?", (team_id,))
        row = cursor.fetchone()

        if not row:
            return None

        columns = [column[0] for column in cursor.description]
        return dict(zip(columns, row))

    finally:
        cursor.close()


def insert_team(team_data: TeamCreate, conn) -> int:
    """
    Execute sp_team_insert stored procedure
    Returns: True if inserted successfully, False otherwise
    """
    cursor = conn.cursor()
    try:
        # Just execute the stored procedure - don't expect any return
        cursor.execute("EXEC sp_team_insert @name = ?", team_data.name)
        cursor.commit()
        
        # Check if any row was affected
        return cursor.rowcount > 0
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def db_update_team(team_data: TeamUpdate, conn) -> bool:
    """
    Execute sp_team_update stored procedure
    Returns: True if updated, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC sp_team_update  @id = ?, @name = ?",
            team_data.id, team_data.name
        )
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def db_delete_team(team_id: int, conn) -> bool:
    """
    Delete team by id (you might need to create this SP)
    Returns: True if deleted, False if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM Team WHERE id = ?", team_id)
        cursor.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def get_transaction_data(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetTransactionData ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_refused_data(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetRefusedData ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_nextech_data(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetNextechByDateRange ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_modmed_data(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetModmedByDateRange ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

     # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_agent_login_by_date(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetAgentLoginByDaterange ?,?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_break_data_by_date_range(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetAgentBreakDataByDateRange ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_time_on_status_by_date_range(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetAgentTimeOnStatusByDateRange ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def get_fssc_data_by_date_range(data, conn):

    cursor = conn.cursor()

    cursor.execute(
        "EXEC sp_GetFSSCDataByDateRange ?, ?, ?, ?",
        data.start_date,
        data.end_date,
        data.page,
        data.page_size
    )

    # First result set (total rows)
    total_rows = cursor.fetchone()[0]

    # Move to next result set
    cursor.nextset()

    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()

    return rows,total_rows

def process_delete_reports(data, conn):

    cursor = conn.cursor()
    result = []
    
    try:
        shiftdate = data.shiftdate
        reports = data.reports

        for report in reports:

            try:
                mapping = REPORT_TABLE_MAP.get(report.lower())

                if not mapping:
                    result.append({
                        "report": report,
                        "message": "Invalid report name"
                    })
                    continue

                table, date_column = mapping

                # Special logic for submission
                if report.lower() == "submission":

                    start_date = shiftdate.replace(day=1)

                    last_day = calendar.monthrange(shiftdate.year, shiftdate.month)[1]
                    end_date = shiftdate.replace(day=last_day)

                else:
                    start_date = shiftdate
                    end_date = shiftdate

                cursor.execute(
                    "EXEC sp_DeleteReportByDateRange ?, ?, ?, ?",
                    table,
                    date_column,
                    start_date,
                    end_date
                )

                result.append({
                    "report": report,
                    "start_date": start_date,
                    "end_date": end_date,
                    "message": "Deleted successfully"
                })

            except Exception as report_error:

                result.append({
                    "report": report,
                    "error": str(report_error)
                })

        conn.commit()

    except Exception as e:
        conn.rollback()

        return {
            "message": "Delete operation failed",
            "error": str(e)
        }

    finally:
        cursor.close()

    return {"result": result}

def process_update_time_on_status(data, conn, user_id):

    cursor = conn.cursor()
    cursor.fast_executemany = True

    validated_rows = []
    error_logs = []
    message = ""

    try:

        validated = UpdateAgentTimeOnStatusRequest(
            Id=data.Id,
            StartTime=data.StartTime,
            EndTime=data.EndTime,
            AvailableTime=data.AvailableTime,
            AvailableTimePercent=data.AvailableTimePercent,
            HandlingTime=data.HandlingTime,
            HandlingTimePercent=data.HandlingTimePercent,
            WrapUpTime=data.WrapUpTime,
            WrapUpTimePercent=data.WrapUpTimePercent,
            WorkingOfflineTime=data.WorkingOfflineTime,
            WorkingOfflineTimePercent=data.WorkingOfflineTimePercent,
            OfferingTime=data.OfferingTime,
            OfferingTimePercent=data.OfferingTimePercent,
            OnBreakTime=data.OnBreakTime,
            OnBreakTimePercent=data.OnBreakTimePercent,
            BusyTime=data.BusyTime,
            BusyTimePercent=data.BusyTimePercent,
            LoggedInTime=data.LoggedInTime,
            Notes=data.Notes,
            updated_by=user_id
        )

        validated_rows.append((
            validated.Id,
            validated.StartTime,
            validated.EndTime,
            validated.AvailableTime,
            validated.AvailableTimePercent,
            validated.HandlingTime,
            validated.HandlingTimePercent,
            validated.WrapUpTime,
            validated.WrapUpTimePercent,
            validated.WorkingOfflineTime,
            validated.WorkingOfflineTimePercent,
            validated.OfferingTime,
            validated.OfferingTimePercent,
            validated.OnBreakTime,
            validated.OnBreakTimePercent,
            validated.BusyTime,
            validated.BusyTimePercent,
            validated.LoggedInTime,
            validated.Notes,
            validated.updated_by
        ))

    except Exception as e:

        error_logs.append((
            data.Id if data.Id else None,
            "Validation",
            str(e),
            json.dumps(data.model_dump(), default=str)
        ))

    if validated_rows:
        try:

            cursor.executemany("""
                EXEC sp_UpdateAgentTimeOnStatus ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            """, validated_rows)

            message = "Agent Time On Status updated successfully"

        except Exception as e:

            for row_data in validated_rows:
                error_logs.append((
                    None,
                    "Database",
                    str(e),
                    json.dumps(row_data, default=str)
                ))

    if error_logs:

        cursor.executemany("""
            EXEC logs ?, ?, ?, ?
        """, error_logs)

        return {
                "message": "Update operation failed",
                "error": str(e)
            }

    conn.commit()
    cursor.close()

    return {"message": message}

def process_update_break_data(data, conn, user_id):

    cursor = conn.cursor()
    cursor.fast_executemany = True

    validated_rows = []
    error_logs = []
    message = ""

    try:
        validated = UpdateBreakDataSchema(
            id=data.id,
            StartTime=data.StartTime,
            EndTime=data.EndTime,
            Status=data.Status,
            StatusCodeItem=data.StatusCodeItem,
            StatusCodeList=data.StatusCodeList,
            TimeValue=data.TimeValue,
            TimePercentage=data.TimePercentage,
            LoggedInTime=data.LoggedInTime,
            Notes=data.Notes,
            updated_by=user_id
        )

        validated_rows.append((
            validated.id,
            validated.StartTime,
            validated.EndTime,
            validated.Status,
            validated.StatusCodeItem,
            validated.StatusCodeList,
            validated.TimeValue,
            validated.TimePercentage,
            validated.LoggedInTime,
            validated.Notes,
            validated.updated_by
        ))

    except Exception as e:
        error_logs.append((
            None,
            "Validation",
            str(e),
            json.dumps(data.model_dump(), default=str)
        ))

    if validated_rows:
        try:

            cursor.executemany("""
                EXEC sp_UpdateAgentBreakData ?,?,?,?,?,?,?,?,?,?,?
            """, validated_rows)

            message = "Break data updated successfully"

        except Exception as e:
            for row_data in validated_rows:
                error_logs.append((
                    None,
                    "Database",
                    str(e),
                    json.dumps(row_data, default=str)
                ))

    if error_logs:
        cursor.executemany("""
            EXEC logs ?, ?, ?, ?
        """, error_logs)

        return {
                "message": "Update operation failed",
                "error": str(e)
            }

    conn.commit()
    cursor.close()

    return {"message": message}

def process_update_login_data(data, conn, user_id):
     
    cursor = conn.cursor()
    cursor.fast_executemany = True

    validated_rows = []
    error_logs = []
    message = ""

    try:
        validated = UpdateLoginRequest(
            id=data.id,
            Login_Time=data.Login_Time,
            Logout_Time=data.Logout_Time,
            Duration=data.Duration,
            Notes=data.Notes,
            updated_by=user_id
        )

        validated_rows.append(tuple(validated.model_dump().values()))

    except Exception as e:
        error_logs.append((
            int(data.id) if data.id else None,
            "Validation",
            str(e),
            json.dumps(data.model_dump(), default=str)
        ))

    if validated_rows:
        try:
            cursor.executemany("""
                EXEC sp_UpdateAgentLoginTime ?, ?, ?, ?,?,?
            """, validated_rows)
            message = "Login data updated successfully"

        except Exception as e:
            for row_data in validated_rows:
                error_logs.append((
                    None,
                    "Database",
                    str(e),
                    json.dumps(row_data, default=str)
                ))

    if error_logs:
            cursor.executemany("""
                    EXEC logs ?, ?, ?, ?
                """, error_logs)
            return {
                "message": "Update operation failed due to database error",
                "error": str(e)
            }
    
    conn.commit()
    cursor.close()
    return {"message": message}

def process_excel_logindata(file_path, conn, user_id):

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
                    Duration=str(row["Duration"]),
                    user_id=user_id
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
                    EXEC insert_agent ?, ?, ?, ?, ?, ?, ?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_daily_breakdata(file_path, conn, user_id):

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
                    LoggedInTime=str(row["LoggedInTime"]),
                    user_id=user_id
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
                    EXEC sp_InsertAgentBreak_Data ?,?,?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_time_on_status(file_path, conn, user_id):

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
                    LoggedInTime=row["LoggedInTime"],
                    user_id=user_id
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
                    EXEC sp_InsertAgentTimeOnStatus ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_refused(file_path, conn, user_id):

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
                    StartTime=row["StartTime"],
                    EndTime=row["EndTime"],
                    Agent=row["Agent"],
                    AgentId=row["AgentId"],
                    Accepted=row["Accepted"],
                    presented=row["Presented"],
                    Rejected=row["Rejected"],
                    AcceptedPercent=row["AcceptedPercent"],
                    RejectedPercent=row["RejectedPercent"],
                    AverageHandlingTime=row["AverageHandlingTime"],
                    AverageWrapUpTime=row["AverageWrapUpTime"],
                    AverageBusyTime=row["AverageBusyTime"],
                    user_id=user_id
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
                    EXEC sp_InsertRefused ?,?,?,?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_transaction_data(file_path, conn, user_id):

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
                    WrapUpCodeText=str(row["WrapUpCodeText"]),
                    user_id=user_id
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
                    EXEC sp_InsertTransactionData ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_form_submission_data(file_path, conn, user_id):

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
                    LastTouchUser=str(row["LastTouchUser"]),
                    user_id=user_id
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
                    EXEC sp_InsertFSSCData ?,?,?,?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_modmed_data(file_path1, file_path2, conn, user_id):

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
                    PrimaryProvider=str(row["PrimaryProvider"]),
                    user_id=user_id
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
                    EXEC sp_InsertModmed ?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }

def process_excel_nextch_data(file_path, conn, user_id):

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
                    user_name=str(row["user_name"]) if not pd.isna(row["user_name"]) else None,
                    user_id=user_id
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
                    EXEC sp_InsertNextech ?,?,?,?,?,?,?,?,?,?
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
        if error_logs:
            return {
                    "message": "upload operation failed due to database error",
                    "error": str(error_logs[0][2])
                }

    return {
        "total": total_rows,
        "inserted": total_inserted,
        "failed": total_failed
    }