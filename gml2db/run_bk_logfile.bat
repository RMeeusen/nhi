echo off
echo arguments: %1 %2
set version=v12
set status=test
set workdir=D:\nhi_run_dir

for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YY=%dt:~2,2%" & set "YYYY=%dt:~0,4%" & set "MM=%dt:~4,2%" & set "DD=%dt:~6,2%"
set "HH=%dt:~8,2%" & set "Min=%dt:~10,2%" & set "Sec=%dt:~12,2%"
set "datestamp=%YYYY%%MM%%DD%" & set "timestamp=%HH%%Min%%Sec%"

if %1 == prod set status=prod
if %2 == v13 set version=v13

echo parsed: version=%version% status=%status%

set newname=%status%_%datestamp%_%timestamp%.log
set oldname=%workdir%\%version%\logs\%status%.log
echo renaming %oldname% to %newname%
ren  %oldname% %newname%

echo delete files older than 10 days
set logdir=%workdir%\%version%\logs\
forfiles /p %logdir% /m *.* /D -10 /C "cmd /c del @file"