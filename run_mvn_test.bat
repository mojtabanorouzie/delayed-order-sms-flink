@echo off
set "JAVA_HOME=C:\Program Files\Java\jdk-17.0.2"
set "PATH=C:\Program Files\Java\jdk-17.0.2\bin;%PATH%"
call mvn test -f flink-job/pom.xml -Dtest=DelayedOrderProcessFunctionTest,OrderStateDeserializationFunctionTest,JobConfigTest