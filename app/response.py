from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

def api_response(status, message=None, data=None, status_code=200):

    return JSONResponse(
        status_code=status_code,
        content={
            "status": status,
            "message": message,
            "data": jsonable_encoder(data)
        }
    )