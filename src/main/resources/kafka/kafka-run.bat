rem Copy file to root folder of kafka

set JAVA_HOME=C:\.jdks\temurin-17.0.13
set TOPIC=quickstart-sber-demo
set PARTITIONS=3

rmdir /s /q \tmp

start bin\windows\zookeeper-server-start.bat config\zookeeper.properties
timeout 15 > NUL
start bin\windows\kafka-server-start.bat config\server.properties
timeout 15 > NUL
start bin\windows\kafka-topics.bat --create --partitions %PARTITIONS% --topic %TOPIC% --bootstrap-server localhost:9092
timeout 10 > NUL
start bin\windows\kafka-console-producer.bat --topic %TOPIC% --bootstrap-server localhost:9092
start bin\windows\kafka-console-consumer.bat --topic %TOPIC% --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property print.partition=true 
bin\windows\kafka-topics.bat --describe --topic %TOPIC% --bootstrap-server localhost:9092

