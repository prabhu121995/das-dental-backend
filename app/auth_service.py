from .db import create_connection

def login_user(username, password):

    with create_connection() as conn:

        cursor = conn.cursor()

        cursor.execute(
            "EXEC sp_LoginUser ?, ?",
            (username, password)
        )

        user = cursor.fetchone()

        if not user:
            return None

        return {
            "user_id": user.user_id,
            "username": user.username,
            "role": user.role_name
        }