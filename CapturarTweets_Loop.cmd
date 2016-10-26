@SET R_Path=C:\Program Files\R\R-3.3.1

@SET R_File=C:\Personal\Dropbox\AFI_JOSE\Proyecto Final\TextMining\CapturarTweets.R

:Loop
@ECHO %DATE% %TIME% - Capturando tweets...>> "%R_File%.out"
"%R_Path%\bin\x64\Rterm.exe" --no-restore --no-save --slave < "%R_File%" >> "%R_File%.out" 2>&1
@IF NOT ERRORLEVEL 1 @GOTO Loop

@ECHO %DATE% %TIME% - FIN>> "%R_File%.out"
