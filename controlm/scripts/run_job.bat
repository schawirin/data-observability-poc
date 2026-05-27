@echo off
REM run_job.bat - Windows wrapper for run_job.py.
REM
REM Use when B3 agents are Windows. Resolves the Python interpreter in this order:
REM   1. %VENV_PATH%\Scripts\python.exe (if VENV_PATH is set)
REM   2. %PYTHON_BIN% (if set, e.g. C:\Python311\python.exe)
REM   3. py -3 (Windows launcher; standard on most Windows installs)
REM   4. python (PATH fallback)
REM
REM Control-M Job:Script can call this directly:
REM   FileName=run_job.bat  FilePath=C:\scripts  Arguments=--job X --date %%ODATE%%

setlocal

set SCRIPT_DIR=%~dp0
set RUN_PY=%SCRIPT_DIR%run_job.py

if defined VENV_PATH (
  if exist "%VENV_PATH%\Scripts\python.exe" (
    "%VENV_PATH%\Scripts\python.exe" "%RUN_PY%" %*
    exit /b %ERRORLEVEL%
  )
)

if defined PYTHON_BIN (
  "%PYTHON_BIN%" "%RUN_PY%" %*
  exit /b %ERRORLEVEL%
)

where py.exe >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  py -3 "%RUN_PY%" %*
  exit /b %ERRORLEVEL%
)

python "%RUN_PY%" %*
exit /b %ERRORLEVEL%
