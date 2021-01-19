echo off
echo arguments: %1 %2 %3
set action=""
set status=test
set version=v12
set workdir=d:\nhi_run_dir
if %1 == get set action=get
if %1 == put set action=put
if %2 == prod set status=prod
set version=%3

echo parsed: action=%action% status=%status%

if %action% == "" (
    echo Missing or unsupported action paramater: %1
    GOTO :usage
) else (
    GOTO :start
)

:usage
echo "Usage: run-sftp-script.bat <action> <status>"

echo action: put or get
echo status: test or prod (default: test)
GOTO :end

:start

for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YY=%dt:~2,2%" & set "YYYY=%dt:~0,4%" & set "MM=%dt:~4,2%" & set "DD=%dt:~6,2%"
set "HH=%dt:~8,2%" & set "Min=%dt:~10,2%" & set "Sec=%dt:~12,2%"
set "datestamp=%YYYY%%MM%%DD%" & set "timestamp=%HH%%Min%%Sec%"

set newname=sftp-%action%-%status%_%datestamp%_%timestamp%.log
set logFile=%workdir%\%version%\logs\%newname%
set script=%workdir%\gml2db\ftp\sftp-%action%-%status%-script.txt

setlocal enabledelayedexpansion
for /F "tokens=1,2,3 delims=;" %%a in (%workdir%\gml2db\config\ftpaccounts.txt) do (    
     
     echo region: %%a  user: %%b password: ****

     if NOT %%a == regio (     
        set destDir=%workdir%\%version%\%status%\%%a     
        "c:\Program Files (x86)\WinSCP\WinSCP.com" /log="%logFile%" /loglevel=-1 /script="%script%" /parameter %%b %%c !destDir!
     ) 

)

:end
echo End of script >> "%logFile%"