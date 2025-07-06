@echo off

@REM :: Save the current directory to a variable
set currentDirectory=%CD%

if /I "%~1" == "/?" goto :usage

@REM :: set session environment variables
if "%~1" == "" (set ENVIRONMENT=sbx) else (set ENVIRONMENT=%~1)
if "%~2" == "" (set LOG_LEVEL=warning) else (set LOG_LEVEL=%~2)
if /I "%~3" == "FORCE_LOAD" (set FORCE_LOAD=True) else (set FORCE_LOAD=False)
if /I "%~4" == "FILE_ARCHIVE" (set FILE_ARCHIVE=True) else (set FILE_ARCHIVE=False)
@REM echo ENVIRONMENT = %ENVIRONMENT%, LOG_LEVEL = %LOG_LEVEL%
@REM echo FORCE_LOAD = %FORCE_LOAD%, FILE_ARCHIVE = %FILE_ARCHIVE%
@REM goto :eof

@REM :: target python script path
set disk=D:
set script_dir=D:\_Data\Working\Script\AWS_Files_to_Datasphere
set py_script=aws-files-to-ds.py

@REM :: Start
%disk% > nul
cd /d %script_dir%
call .venv\Scripts\activate.bat
python %py_script% %*
set PythonScriptResultCode=%ERRORLEVEL%
call .venv\Scripts\deactivate.bat

@REM :: Alternatively
@REM :: Save the current directory and change to a new one
@REM :: pushd C:\Windows

@REM :: Perform operations in the new directory
@REM :: ...

@REM :: Return to the previously saved directory
@REM :: popd

@REM :: Restore the original directory using the saved variable
cd /d %currentDirectory%

@REM :: End
if %PythonScriptResultCode% neq 0 goto :error

:success
echo Script successful!
exit /b 0

:error
echo Script failed!
exit /b 1

:usage
echo Please provide desired parameter(s) to run Python script ...
echo Usage: script.bat /? to display this help
echo        script.bat [environment] [log level] [force_load] [file_archive]
echo All parameters are optional and case-insensitive, but in order;
echo If parameter not specified value will be assigned default value.
echo    Environments : sbx, dev, uat, qa, prd, provid; default is sbx
echo    Log levels   : notset, info, warning, debug, error, critical; default is warning.
echo    force_load   : skip load condiction to force load all files; default is False.
echo    file_archive : archive file once successful loaded; default is False.
echo Examples: 
echo    script.bat
echo    script.bat dev
echo    script.bat dev info
echo    script.bat uat error force_load
echo    script.bat sbx warning force_load file_archive
