@echo off
setlocal
set "PATH=C:\msys64\ucrt64\bin;%PATH%"
"C:\msys64\ucrt64\bin\dlltool.exe" %*
exit /b %ERRORLEVEL%
