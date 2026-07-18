@echo off
title Smart City Mobility Intelligence Platform - Shutdown Control
color 0E

echo =======================================================================
echo     SMART CITY MOBILITY INTELLIGENCE PLATFORM - SHUTDOWN PANEL
echo =======================================================================
echo.
echo Stopping all running containers and clearing resources...
cd "%~dp0"
docker-compose -f docker/docker-compose.yml down
if %errorlevel% neq 0 (
    color 0C
    echo ERROR: Failed to stop Docker Compose services.
    echo.
    pause
    exit /b
)

color 0A
echo.
echo =======================================================================
echo SUCCESS: All platform services have been stopped successfully!
echo =======================================================================
echo.
timeout /t 3 >nul
exit
