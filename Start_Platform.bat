@echo off
title Smart City Mobility Intelligence Platform - Startup Control
color 0B

echo =======================================================================
echo     SMART CITY MOBILITY INTELLIGENCE PLATFORM - CONTROL PANEL
echo =======================================================================
echo.
echo [1/3] Checking if Docker is running...
docker info >nul 2>&1
if %errorlevel% neq 0 (
    color 0C
    echo ERROR: Docker Desktop is not running!
    echo Please open Docker Desktop and try again.
    echo.
    pause
    exit /b
)
echo Docker is running.
echo.
echo [2/3] Launching platform services via Docker Compose...
cd "%~dp0"
docker-compose -f docker/docker-compose.yml up -d --build
if %errorlevel% neq 0 (
    color 0C
    echo ERROR: Failed to start Docker Compose services.
    echo.
    pause
    exit /b
)
echo.
echo [3/3] Opening Web Interfaces in your browser...
echo Services are booting up. Opening dashboards now:
echo - Airflow Dashboard: http://localhost:8085 (admin / admin)
echo - Spark Master UI:   http://localhost:8090
echo - FastAPI Swaggers:  http://localhost:8000/docs
echo.

:: Small delay to allow port binding
timeout /t 5 >nul

start http://localhost:8085
start http://localhost:8090
start http://localhost:8000/docs

echo =======================================================================
echo SUCCESS: Platform is running!
echo You can close this window. To stop the platform, run Stop_Platform.bat.
echo =======================================================================
echo.
pause
