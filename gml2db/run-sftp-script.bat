echo off
echo arguments: %1 %2
set action=""
set status=test
if %1 == get set action=get
if %1 == put set action=put
if %2 == prod set status=prod

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
set newname=sftp-%action%-%status%-%date:/=-%_%time::=-%.log
set logFile=d:\nhi\gml2db\logs\%newname%
set script=d:\nhi\gml2db\ftp\sftp-%action%-%status%-script.txt

for /F "tokens=1,2,3 delims=;" %%a in (d:\nhi\gml2db\config\ftpaccounts.txt) do (    
     
     echo region: %%a  user: %%b password: ****

     if NOT %%a == regio (     
        echo Get %%a files >> "%logFile%"
        "c:\Program Files (x86)\WinSCP\WinSCP.com" /log="%logFile%" /loglevel=-1 /script="%script%" /parameter %%b %%c %%a 
     ) 

)

:end
echo End of script >> "%logFile%"