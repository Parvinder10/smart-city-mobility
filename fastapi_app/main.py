# main.py
import random
from datetime import datetime
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(
    title="Smart City Mobility API Gateways",
    description="Provides real-time endpoints for weather reports and road incident feeds.",
    version="1.0.0"
)

# Mock databases
WEATHER_STATIONS = ["WTH-CENTRAL", "WTH-JFK", "WTH-LGA"]
INCIDENTS_DB = [
    {
        "incident_id": 1001,
        "incident_type": "Accident",
        "severity": "MAJOR",
        "location_name": "Broadway & 42nd St",
        "latitude": 40.75797,
        "longitude": -73.98554,
        "record_timestamp": datetime.now().isoformat()
    },
    {
        "incident_id": 1002,
        "incident_type": "Road Construction",
        "severity": "MINOR",
        "location_name": "FDR Dr & E 96th St",
        "latitude": 40.78166,
        "longitude": -73.94052,
        "record_timestamp": datetime.now().isoformat()
    }
]

class IncidentReport(BaseModel):
    incident_type: str  # Accident, Construction, Breakdown, Event
    severity: str      # MINOR, MAJOR, CRITICAL
    location_name: str
    latitude: float
    longitude: float

@app.get("/weather", response_model=List[Dict[str, Any]])
def get_weather():
    """Generates and returns mock real-time weather data for city weather stations."""
    weather_data = []
    for station in WEATHER_STATIONS:
        # Generate weather conditions based on station
        temp = round(random.uniform(32, 95), 1)  # Fahrenheit
        precip = round(random.uniform(0, 2.5) if random.random() > 0.7 else 0.0, 2)
        wind = round(random.uniform(2, 25), 1)
        weather_data.append({
            "station_id": station,
            "temperature": temp,
            "precipitation": precip,
            "wind_speed": wind,
            "record_timestamp": datetime.now().isoformat()
        })
    return weather_data

@app.get("/incidents", response_model=List[Dict[str, Any]])
def get_incidents():
    """Returns the list of active road incidents."""
    return INCIDENTS_DB

@app.post("/incidents", status_code=201)
def create_incident(incident: IncidentReport):
    """Allows reporting of new incidents (e.g. from city patrol vehicles)."""
    if incident.severity not in ["MINOR", "MAJOR", "CRITICAL"]:
        raise HTTPException(status_code=400, detail="Invalid severity level. Must be MINOR, MAJOR, or CRITICAL.")
    
    new_id = max([i["incident_id"] for i in INCIDENTS_DB], default=1000) + 1
    new_incident = {
        "incident_id": new_id,
        "incident_type": incident.incident_type,
        "severity": incident.severity,
        "location_name": incident.location_name,
        "latitude": incident.latitude,
        "longitude": incident.longitude,
        "record_timestamp": datetime.now().isoformat()
    }
    INCIDENTS_DB.append(new_incident)
    return {"message": "Incident reported successfully", "incident": new_incident}

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}
