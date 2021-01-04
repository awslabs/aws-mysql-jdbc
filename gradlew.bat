@rem
@rem AWS JDBC Driver for MySQL
@rem Copyright 2020 Amazon.com Inc. or affiliates.
@rem
@rem This program is free software; you can redistribute it and/or modify it under
@rem the terms of the GNU General Public License, version 2.0, as published by the
@rem Free Software Foundation.
@rem
@rem This program is also distributed with certain software (including but not
@rem limited to OpenSSL) that is licensed under separate terms, as designated in a
@rem particular file or component or in included license documentation. The
@rem authors of MySQL hereby grant you an additional permission to link the
@rem program and your derivative works with the separately licensed software that
@rem they have included with MySQL.
@rem
@rem Without limiting anything contained in the foregoing, this file, which is
@rem part of this connector, is also subject to the Universal FOSS Exception,
@rem version 1.0, a copy of which can be found at
@rem http://oss.oracle.com/licenses/universal-foss-exception.
@rem
@rem This program is distributed in the hope that it will be useful, but WITHOUT
@rem ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
@rem FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
@rem for more details.
@rem
@rem You should have received a copy of the GNU General Public License along with
@rem this program; if not, write to the Free Software Foundation, Inc.,
@rem 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
@rem 

@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  Gradle startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and GRADLE_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS="-Xmx64m" "-Xms64m"

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windows variants

if not "%OS%" == "Windows_NT" goto win9xME_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\gradle\wrapper\gradle-wrapper.jar


@rem Execute Gradle
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %GRADLE_OPTS% "-Dorg.gradle.appname=%APP_BASE_NAME%" -classpath "%CLASSPATH%" org.gradle.wrapper.GradleWrapperMain %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable GRADLE_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%GRADLE_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
