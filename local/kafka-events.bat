C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-console-producer.bat --topic event --bootstrap-server localhost:9092
C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-topics.bat --create --topic organization-closure-date --partitions 10 --bootstrap-server localhost:9092
C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-topics.bat --create --topic organization-parameters --partitions 10 --bootstrap-server localhost:9092
C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-topics.bat --create --topic event --partitions 10 --bootstrap-server localhost:9092
C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-topics.bat --create --topic output-failure --partitions 10 --bootstrap-server localhost:9092
C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-topics.bat --create --topic output-success --partitions 10 --bootstrap-server localhost:9092
C:\dev\tools\kafka_2.12-2.5.1\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
