# Kafka springboot demo Tracking

## Description

This project is running with https://github.com/johndbs/spring-kafka-demo


## Kafka
```bash
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic dispatch.tracking
```
Message example:
```json
{"orderId": "f6ade72d-d6d2-4bbb-8543-08a35b830884"}
```

Read message genereated by the application:
```bash 
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic tracking.status
```
