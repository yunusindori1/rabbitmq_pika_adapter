@echo off
setlocal

cd /d F:\DevHome\PythonDev\rabbit_mq_client

REM ---- Integration env (only for this process) ----
set RABBITMQ_HOST=192.168.2.25
set RABBITMQ_PORT=5672
set RABBITMQ_VHOST=tradier
set RABBITMQ_USER=tradier_mq_admin
set RABBITMQ_PASSWORD=default123

REM ---- Run all tests (integration tests will no longer skip) ----
python -m pytest -q

endlocal
