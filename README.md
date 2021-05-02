## Starting the Kafka 

kafka version: kafka_2.11-2.4.0

1. Start Zookeeper
 
    * Linux:    `bin/zookeeper-server-start.sh config/zookeeper.properties`
    * Windows:  `.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties`

2. Start Kafka Server

    * Linux:   `bin/kafka-server-start.bat config/server.properties`
    * Windows: `.\bin\windows\kafka-server-start.sh .\config\server.properties`

## Producer and Consumer

1. Producer
    * Linus:    `bin/kafka-console-producer.sh --topic test-topic --broker-list localhost:9092`
    * Windows:  `.\bin\windows\kafka-console-producer.bat --topic test-topic --broker-list localhost:9092`

2. Consumer
    * Linus:    `bin/kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092`
    * Windows:  `.\bin\windows\kafka-console-consumer.bat --topic test-topic --from-beginning --bootstrap-server localhost:9092`

## Shut down Kafka and Zookeeper

1. Kafka
    * Linux:    `bin/kafka-server-stop.sh`
    * Windows:  `.\bin\windows\kafka-server-stop.bat`

2. Zookeeper
    * Linux:    `bin/zookeeper-server-stop.sh`
    * Windows:  `.\bin\windows\zookeeper-server-stop.bat`