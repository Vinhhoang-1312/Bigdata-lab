@echo off
setlocal EnableExtensions

cd /d "%~dp0"
set "URBANFLOW_ROOT=%CD%"
set "NPM_CONFIG_CACHE=D:\UrbanFlow-npm-cache"

echo [1/4] Checking Docker Desktop...
docker info >nul 2>&1
if errorlevel 1 goto :docker_error

echo [2/4] Starting storage, Spark, and API services...
docker compose up -d seaweedfs spark-master spark-worker-1 spark-worker-2
if errorlevel 1 goto :compose_error
rem Recreate source-mounted APIs so pulled or merged routes cannot stay stale.
docker compose up -d --force-recreate streaming-api ml-api
if errorlevel 1 goto :compose_error

echo [3/4] Starting the React application...
if not exist "%URBANFLOW_ROOT%\urbanflow-ui\node_modules\.bin\vite.cmd" (
  echo Installing React dependencies with the npm cache on D:...
  pushd "%URBANFLOW_ROOT%\urbanflow-ui"
  call npm.cmd ci --cache "%NPM_CONFIG_CACHE%"
  if errorlevel 1 (
    popd
    goto :npm_error
  )
  popd
)

call :url_ready "http://127.0.0.1:5173/streaming"
if errorlevel 1 (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath 'npm.cmd' -ArgumentList 'run','dev' -WorkingDirectory '%URBANFLOW_ROOT%\urbanflow-ui' -WindowStyle Hidden"
  if errorlevel 1 goto :react_start_error
)

echo [4/4] Waiting for the applications...
call :wait_for_url "http://127.0.0.1:8001/openapi.json" "Streaming API"
if errorlevel 1 goto :service_error
call :wait_for_url "http://127.0.0.1:8002/openapi.json" "ML API"
if errorlevel 1 goto :service_error
call :wait_for_url "http://127.0.0.1:5173/streaming" "React UI"
if errorlevel 1 goto :service_error
call :wait_for_url "http://127.0.0.1:8080" "Spark Master UI"
if errorlevel 1 goto :service_error

echo.
echo URBANFLOW READY
echo React Streaming: http://127.0.0.1:5173/streaming
echo React ML:        http://127.0.0.1:5173/ml
echo Spark Master:    http://127.0.0.1:8080

if /I not "%URBANFLOW_SKIP_OPEN%"=="1" (
  start "" "http://127.0.0.1:5173/streaming"
  start "" "http://127.0.0.1:5173/ml"
  start "" "http://127.0.0.1:8080"
)

endlocal
exit /b 0

:url_ready
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "try { $response = Invoke-WebRequest -UseBasicParsing -Uri '%~1' -TimeoutSec 3; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { exit 0 }; exit 1 } catch { exit 1 }" >nul 2>&1
exit /b %errorlevel%

:wait_for_url
setlocal
set "WAIT_URL=%~1"
set "WAIT_NAME=%~2"
for /L %%I in (1,1,60) do (
  call :url_ready "%WAIT_URL%"
  if not errorlevel 1 (
    echo [OK] %WAIT_NAME%
    endlocal & exit /b 0
  )
  timeout /t 1 /nobreak >nul
)
echo [ERROR] %WAIT_NAME% did not become ready: %WAIT_URL%
endlocal & exit /b 1

:docker_error
echo [ERROR] Docker Desktop is not running or is not ready.
goto :failed

:compose_error
echo [ERROR] Docker Compose could not start the UrbanFlow services.
goto :failed

:npm_error
echo [ERROR] npm could not install the React dependencies.
goto :failed

:react_start_error
echo [ERROR] The React development server could not be started.
goto :failed

:service_error
echo [ERROR] One or more UrbanFlow services did not become ready.
goto :failed

:failed
if /I not "%URBANFLOW_NONINTERACTIVE%"=="1" pause
endlocal
exit /b 1
