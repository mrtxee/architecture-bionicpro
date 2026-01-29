import os
from datetime import datetime
from typing import Dict, Optional

import requests
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# OLAP Postgres
POSTGRES_DSN = os.getenv("OLAP_DSN", "postgresql://airflow:airflow@localhost:5434/olap")

# Keycloak
KEYCLOAK_SERVER_URL = os.getenv("KEYCLOAK_SERVER_URL", "http://localhost:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "reports-api")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

jwks_url = f"{KEYCLOAK_SERVER_URL}realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
jwks = requests.get(jwks_url).json()


def get_jwk(kid: str):
    for key in jwks["keys"]:
        if key["kid"] == kid:
            return key
    raise HTTPException(status_code=401, detail="Invalid token key")


def decode_jwt(token: str) -> dict:
    try:
        header = jwt.get_unverified_header(token)
        jwk = get_jwk(header["kid"])
        payload = jwt.decode(
            token, jwk, algorithms=["RS256"], audience=KEYCLOAK_CLIENT_ID
        )
        return payload
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


def get_current_user(token: str = Depends(oauth2_scheme)):
    return decode_jwt(token)


engine = create_engine(POSTGRES_DSN)
SessionLocal = sessionmaker(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/reports")
def get_report(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    current_user: dict = Depends(get_current_user),
):
    session = SessionLocal()
    try:
        username = current_user.get("preferred_username")
        if not username:
            username = current_user.get("sub")

        try:
            start_dt = datetime.fromisoformat(start_date) if start_date else None
            end_dt = datetime.fromisoformat(end_date) if end_date else None
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format, use YYYY-MM-DD"
            )
        query_text = """
                    SELECT username, email, date_of_birth, timestamp, sensor_value
                    FROM report
                    WHERE username = :username
                """
        params = {"username": username}

        if start_dt:
            query_text += " AND timestamp >= :start_date"
            params["start_date"] = start_dt
        if end_dt:
            query_text += " AND timestamp <= :end_date"
            params["end_date"] = end_dt

        query_text += " ORDER BY timestamp ASC"
        result = session.execute(text(query_text), params).fetchall()

        if not result:
            return {
                "reports": [],
                "message": "Отсутствуют данные за период.",
            }
        reports = []
        for row in result:
            reports.append(
                {
                    "username": row.username,
                    "email": row.email,
                    "date_of_birth": row.date_of_birth.isoformat()
                    if row.date_of_birth
                    else None,
                    "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                    "sensor_value": float(row.sensor_value)
                    if row.sensor_value is not None
                    else None,
                }
            )
        return {"reports": reports}
    finally:
        session.close()
