@echo off
setlocal EnableDelayedExpansion

cd /d %RABBITMQ_PIKA_ADAPTER_ROOT% || (
    echo ERROR: RABBITMQ_PIKA_ADAPTER_ROOT is not set or invalid.
    exit /b 1
)

REM ---- Load .env with validation ----
for /f "usebackq tokens=1,* delims==" %%A in (`findstr /v "^;" .env`) do (
    if "%%A"=="" (
        echo ERROR: Invalid line in .env
        exit /b 1
    )
    if "%%B"=="" (
        echo ERROR: Variable %%A has no value.
        exit /b 1
    )
    set "%%A=%%B"
)

REM ---- Run tests ----
python -m pytest -q

endlocal
