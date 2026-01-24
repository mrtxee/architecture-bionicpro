from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import List, Optional
import uvicorn
from jose import jwt, JWTError

app = FastAPI(title="BionicPRO Reports API", version="1.0.0")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def cors_handler(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

security = HTTPBearer()

class ReportData(BaseModel):
    user_id: str
    patient_name: str
    device_id: str
    amputation_type: Optional[str]
    prosthesis_type: Optional[str]
    order_date: Optional[str]
    delivery_date: Optional[str]
    status: Optional[str]
    training_sessions: Optional[int]
    response_time_target: Optional[int]
    avg_battery_level: Optional[float]
    avg_response_time: Optional[float]
    avg_muscle_signal_strength: Optional[float]
    avg_actuator_power: Optional[float]
    avg_device_temperature: Optional[float]
    total_usage_duration: Optional[int]
    action_count: Optional[int]
    last_activity_ts: Optional[str]

class ReportResponse(BaseModel):
    success: bool
    data: List[ReportData]
    message: str

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "airflow"),
            user=os.getenv("DB_USER", "airflow"),
            password=os.getenv("DB_PASSWORD", "airflow")
        )
        return conn
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}"
        )


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    
    if not token or token == "invalid":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials"
        )
    
    try:
        payload = jwt.get_unverified_claims(token)
        
        user_id = payload.get('preferred_username') or payload.get('sub')
        
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User ID not found in token"
            )
        
        return {"user_id": user_id}
        
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token format: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Token verification error: {str(e)}"
        )

@app.options("/reports")
async def options_reports():
    return Response(status_code=200)

@app.options("/reports/{user_id}")
async def options_reports_user_id():
    return Response(status_code=200)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "BionicPRO Reports API"}

@app.get("/reports", response_model=ReportResponse)
async def get_reports(user_info: dict = Depends(verify_token)):
    try:
        user_id = user_info["user_id"]
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            user_id,
            patient_name,
            device_id,
            amputation_type,
            prosthesis_type,
            order_date,
            delivery_date,
            status,
            training_sessions,
            response_time_target,
            avg_battery_level,
            avg_response_time,
            avg_muscle_signal_strength,
            avg_actuator_power,
            avg_device_temperature,
            total_usage_duration,
            action_count,
            last_activity_ts
        FROM data_mart 
        WHERE user_id = %s
        ORDER BY created_at DESC
        """
        
        cursor.execute(query, (user_id,))
        results = cursor.fetchall()
        
        reports = []
        for row in results:
            report = ReportData(
                user_id=row['user_id'],
                patient_name=row['patient_name'],
                device_id=row['device_id'],
                amputation_type=row['amputation_type'],
                prosthesis_type=row['prosthesis_type'],
                order_date=str(row['order_date']) if row['order_date'] else None,
                delivery_date=str(row['delivery_date']) if row['delivery_date'] else None,
                status=row['status'],
                training_sessions=row['training_sessions'],
                response_time_target=row['response_time_target'],
                avg_battery_level=float(row['avg_battery_level']) if row['avg_battery_level'] else None,
                avg_response_time=float(row['avg_response_time']) if row['avg_response_time'] else None,
                avg_muscle_signal_strength=float(row['avg_muscle_signal_strength']) if row['avg_muscle_signal_strength'] else None,
                avg_actuator_power=float(row['avg_actuator_power']) if row['avg_actuator_power'] else None,
                avg_device_temperature=float(row['avg_device_temperature']) if row['avg_device_temperature'] else None,
                total_usage_duration=row['total_usage_duration'],
                action_count=row['action_count'],
                last_activity_ts=str(row['last_activity_ts']) if row['last_activity_ts'] else None
            )
            reports.append(report)
        
        cursor.close()
        conn.close()
        
        return ReportResponse(
            success=True,
            data=reports,
            message=f"Found {len(reports)} reports for user {user_id}"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving reports: {str(e)}"
        )

@app.get("/reports/{user_id}", response_model=ReportResponse)
async def get_reports_by_user_id(
    user_id: str, 
    user_info: dict = Depends(verify_token)
):
    if user_info["user_id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: You can only access your own reports"
        )
    
    return await get_reports(user_info)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
