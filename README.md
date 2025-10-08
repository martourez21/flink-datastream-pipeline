# Flink Avro Kafka Pipeline

A simple Apache Flink project that consumes Avro-serialized transactions from a Kafka topic and sinks them to another Kafka topic without any processing operations.

## Prerequisites

- Java 11 or higher
- Apache Maven 3.6+
- Apache Kafka 2.8+
- Confluent Platform (optional, for Schema Registry)
- Apache Flink 1.17+ (optional, for cluster deployment)

## Quick Start

### 1. Clone and Build

```bash
git clone <repository-url>
cd flink-avro-kafka
mvn clean package
```

### 2. Start Kafka Services

```bash
# Start Zookeeper
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

# Start Kafka
kafka-server-start.sh $KAFKA_HOME/config/server.properties

# If using Confluent Platform
confluent local services start
```

### 3. Create Kafka Topics

```bash
# For Avro examples
kafka-topics.sh --create --topic transactions-avro-input \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic transactions-avro-output \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# For simple String example
kafka-topics.sh --create --topic transactions-input \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic transactions-output \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Available Examples

### 1. Simple Avro Pipeline (Recommended)

Uses Flink's built-in Avro serialization without Schema Registry.

```bash
# Run the application
mvn exec:java -Dexec.mainClass="com.example.AvroKafkaPipeline"

# Or submit to Flink cluster
flink run target/flink-avro-kafka-1.0-SNAPSHOT.jar
```

### 2. Custom Avro Serialization Pipeline

Provides manual control over Avro serialization/deserialization.

```bash
mvn exec:java -Dexec.mainClass="com.example.TransactionAvroPipeline"
```

### 3. Simple String Pipeline

For testing without Avro complexity.

```bash
mvn exec:java -Dexec.mainClass="com.example.SimpleKafkaPipeline"
```

## Testing the Pipeline

### For Avro Pipelines:

1. **Produce Avro messages** using the console producer:

```bash
kafka-avro-console-producer \
  --broker-list localhost:9092 \
  --topic transactions-avro-input \
  --property value.schema='{"type":"record","name":"Transaction","namespace":"com.example","fields":[{"name":"transactionId","type":"string"},{"name":"amount","type":"double"},{"name":"currency","type":"string"},{"name":"timestamp","type":"long"},{"name":"merchantId","type":"string"},{"name":"customerId","type":"string"}]}' \
  --property schema.registry.url=http://localhost:8081
```

2. **Input sample data**:
```json
{"transactionId": "txn-001", "amount": 100.50, "currency": "USD", "timestamp": 1698765432000, "merchantId": "merch-001", "customerId": "cust-001"}
{"transactionId": "txn-002", "amount": 250.75, "currency": "EUR", "timestamp": 1698765433000, "merchantId": "merch-002", "customerId": "cust-002"}
```

3. **Consume from output topic**:
```bash
kafka-avro-console-consumer \
  --topic transactions-avro-output \
  --bootstrap-server localhost:9092 \
  --property schema.registry.url=http://localhost:8081 \
  --from-beginning
```

### For Simple String Pipeline:

1. **Produce String messages**:
```bash
kafka-console-producer.sh --topic transactions-input --bootstrap-server localhost:9092
```

2. **Input sample data**:
```
{"transactionId": "txn-001", "amount": 100.50, "currency": "USD"}
{"transactionId": "txn-002", "amount": 250.75, "currency": "EUR"}
```

3. **Consume from output topic**:
```bash
kafka-console-consumer.sh --topic transactions-output --bootstrap-server localhost:9092 --from-beginning
```

## Configuration

### Kafka Connection

Update the following in the Java files if needed:

```java
String bootstrapServers = "localhost:9092";
String schemaRegistryUrl = "http://localhost:8081"; // For Confluent Avro
String inputTopic = "transactions-avro-input";
String outputTopic = "transactions-avro-output";
String consumerGroupId = "flink-avro-consumer";
```

### Avro Schema

The transaction schema is defined in `transaction.avsc`:

```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.example",
  "fields": [
    {"name": "transactionId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "merchantId", "type": "string"},
    {"name": "customerId", "type": "string"}
  ]
}
```

## Monitoring

The application includes console logging to monitor the data flow:

```
Received: txn-001 - Amount: 100.50 USD
Received: txn-002 - Amount: 250.75 EUR
```

## Deployment Options

### 1. Local Execution
```bash
mvn exec:java -Dexec.mainClass="com.example.AvroKafkaPipeline"
```

### 2. Flink Cluster Submission
```bash
flink run target/flink-avro-kafka-1.0-SNAPSHOT.jar
```

### 3. Docker Deployment
```bash
docker build -t flink-avro-kafka .
docker run --network host flink-avro-kafka
```

## Dependencies

- **Apache Flink 1.17.1** - Stream processing engine
- **Apache Kafka** - Message broker
- **Apache Avro 1.11.1** - Data serialization
- **Confluent Schema Registry** (optional) - Avro schema management

## Troubleshooting

### Common Issues

1. **Class not found errors**: Run `mvn clean compile` to regenerate Avro classes
2. **Kafka connection issues**: Verify Kafka is running and bootstrap servers are correct
3. **Avro serialization errors**: Check that the schema matches between producer and consumer
4. **Schema Registry errors**: Ensure Confluent Schema Registry is running if using Confluent Avro

### Logs

Check application logs for detailed error information. Logs are configured in `src/main/resources/log4j2.properties`.

## Development

### Generating Avro Classes

Avro classes are automatically generated during build. To manually generate:

```bash
mvn avro:schema
```

### Adding New Features

1. Extend the Avro schema in `transaction.avsc`
2. Update the Flink job logic in the pipeline classes
3. Add new serializers/deserializers if needed

## License

This project is for demonstration purposes and can be used as a template for Flink-Kafka-Avro integrations.
