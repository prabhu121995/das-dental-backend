from typing import List, Union
from jose import jwt
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer

SECRET_KEY = "supersecret"
ALGORITHM = "HS256"
security = HTTPBearer()

def create_token(user):

    payload = {
        "user_id": user["user_id"],
        "username": user["username"],
        "role": user["role"],
        "exp": datetime.utcnow() + timedelta(hours=8)
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    return token

def get_current_user(token = Depends(security)):

    try:

        payload = jwt.decode(
            token.credentials,
            "supersecret",
            algorithms=["HS256"]
        )

        return payload

    except:
        raise HTTPException(status_code=401, detail="Invalid token")
    
def require_role(roles: Union[str, List[str]]):

     # Convert single role → list
    if isinstance(roles, str):
        roles = [roles]

    def role_checker(user = Depends(get_current_user)):

        if user["role"] not in roles:
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )

        return user

    return role_checker