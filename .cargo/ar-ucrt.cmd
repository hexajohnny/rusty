@echo off
setlocal
set "PATH=C:\msys64\ucrt64\bin;%PATH%"
"C:\msys64\ucrt64\bin\ar.exe" %*
exit /b %ERRORLEVEL%
