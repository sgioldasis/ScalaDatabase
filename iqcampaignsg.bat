@echo off
SET SQLCONNECTION="CBOGIQ_PROD"

SET RESULTDRIVE=D:
SET RESULTDIR="\NetBeansProjects\ScalaDatabase\exe"
SET CAMPAIGN=%1
SET TIME_KEY=%2


%RESULTDRIVE%

CD %RESULTDIR%

java -Xms512m -Xmx1024m -jar "C:\WINNT\dist_scala\ScalaDatabase.jar" +segment NATIVEIQ %SQLCONNECTION% %CAMPAIGN% %TIME_KEY% > CM_%CAMPAIGN%.log
