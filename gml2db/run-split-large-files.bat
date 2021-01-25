echo off
echo arguments: %1 %2
set status=test
set version=v12
set rootdir=d:\nhi_run_dir
set maxbytesize=250000000
if %1 == prod set status=prod
set version=%2
echo parsed: status=%status% version=%version%

for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YY=%dt:~2,2%" & set "YYYY=%dt:~0,4%" & set "MM=%dt:~4,2%" & set "DD=%dt:~6,2%"
set "HH=%dt:~8,2%" & set "Min=%dt:~10,2%" & set "Sec=%dt:~12,2%"
set "datestamp=%YYYY%%MM%%DD%" & set "timestamp=%HH%%Min%%Sec%"

set newname=split-%status%_%datestamp%_%timestamp%.log
set logFile=%rootdir%\%version%\logs\%newname%

set importDir=%rootdir%\%version%\%status%\
echo importDir=%importDir% >> %logFile%

setlocal enabledelayedexpansion
for /D %%i in (%importDir%/*) do (

   set workDir=%%i\WORK\
   IF NOT EXIST !workDir! mkdir !workDir!

   set gmlDir=%%i\GML\
   for %%j in (%%i\GML\*.gml) do (

       FOR /F "usebackq" %%A IN ('%%j') DO set size=%%~zA
       if !size! GTR %maxbytesize% (
          echo Start splitting %%j >> %logFile%
           
          For %%A in ("%%j") do (
             Set file=%%~nxA
         )
  
	 echo Move !file! to !workDir!  >> %logFile%        
         move %%j !workDir!
         java -Xmx1024m -jar d:/nhi/gml2db/splitter/featurecollectionsplitter.jar !workDir!!file! !gmlDir! 25000
	 echo finished splitting >> %logFile%
         del !workDir!/!file!
                   	  
       ) 
    )     
)
:end