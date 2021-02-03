echo off
echo arguments: action=%1 system=%2 version=%3

if "%1" == "" (
    echo Missing or unsupported action paramater: %1
    GOTO :usage
) else (
    GOTO :start
)

:usage
echo "Usage: run-sftp-script.bat <action> <status> <version>"

echo action:  put or get
echo status:  test or prod (default: test)
echo version: v12 or v13   (default: v12)
GOTO :end

:start
set action=""
set status=TEST
set version=V12
set workdir=d:\nhi_run_dir
if %1 == get set action=get
if %1 == put set action=put
if %2 == prod set status=PROD
if %3 == v13 set version=V13

echo parsed: action=%action% status=%status%


for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YY=%dt:~2,2%" & set "YYYY=%dt:~0,4%" & set "MM=%dt:~4,2%" & set "DD=%dt:~6,2%"
set "HH=%dt:~8,2%" & set "Min=%dt:~10,2%" & set "Sec=%dt:~12,2%"
set "datestamp=%YYYY%%MM%%DD%" & set "timestamp=%HH%%Min%%Sec%"

set newname=sftp-%action%-%status%_%datestamp%_%timestamp%.log
set logFile=%workdir%\%version%\logs\%newname%
set script=%workdir%\ftp\sftp-%action%-script.txt

setlocal enabledelayedexpansion
for /F "tokens=1,2,3 delims=;" %%a in (%workdir%\gml2db\config\ftpaccounts.txt) do (    
     
     echo region: %%a  user: %%b password: ****

     if NOT %%a == regio (     
        set nhiDir=%workdir%\%version%\%status%\%%a
        set ftpDir=/%version%/%status%/  

	echo running script %script% with parameters: %%b %%c !ftpDir! !nhiDir!
        "c:\Program Files (x86)\WinSCP\WinSCP.com" /log="%logFile%" /loglevel=-1 /script="%script%" /parameter %%b %%c !ftpDir! !nhiDir!
     ) 

)

:end
echo End of script >> "%logFile%"