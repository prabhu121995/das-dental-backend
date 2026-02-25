from .db import create_connection

def get_db():
    conn = create_connection()
    try:
        yield conn
    finally:
        conn.close()