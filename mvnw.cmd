@REM SPDX-License-Identifier: Apache-2.0
@REM Maven Wrapper bootstrap (Windows). Resolves and runs Maven version specified
@REM in .mvn\wrapper\maven-wrapper.properties.
@echo off
setlocal
set WRAPPER_JAR=.mvn\wrapper\maven-wrapper.jar
set PROPS=.mvn\wrapper\maven-wrapper.properties
if not exist "%WRAPPER_JAR%" (
    for /f "tokens=2 delims==" %%A in ('findstr /B "wrapperUrl=" "%PROPS%"') do set WRAPPER_URL=%%A
    echo Downloading Maven wrapper from %WRAPPER_URL%
    powershell -Command "Invoke-WebRequest -Uri '%WRAPPER_URL%' -OutFile '%WRAPPER_JAR%'"
)
"%JAVA_HOME%\bin\java" -classpath "%WRAPPER_JAR%" ^
    "-Dmaven.multiModuleProjectDirectory=%CD%" ^
    org.apache.maven.wrapper.MavenWrapperMain %*
