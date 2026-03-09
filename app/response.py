from fastapi.responses import JSONResponse

def api_response(status: str, data=None, message: str = None, status_code: int = 200):
    return JSONResponse(
        status_code=status_code,
        content={
            "status": status,
            "message": message,
            "data": data
        }
    )