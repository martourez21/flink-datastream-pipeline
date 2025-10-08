package com.codedstreams.flinkpipeline;

import com.codedstream.transfruad.library.schema.CardTransaction;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataStreamPipelineApplication {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "kafka-01:9092";
        String schemaRegistryUrl = "http://sschemaregistry0-01:8081";
        String inputTopic = "financial-transactions";
        String outputTopic = "processed-transactions";
        String consumerGroupId = "financial-transactions-consumer-group";

        KafkaSource<CardTransaction> source = KafkaSource.<CardTransaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        AvroDeserializationSchema.forSpecific(
                                CardTransaction.class
                        )
                ).build();

        KafkaSink<CardTransaction> sink = KafkaSink.<CardTransaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(
                                        AvroSerializationSchema.forSpecific(CardTransaction.class)
                                )
                                .build()
                )
                .build();

        DataStream<CardTransaction> transactionStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Transaction Source"
        );

        transactionStream.map(transaction ->
                String.format("Received: %s - Amount: %.2f %s",
                        transaction.getTransactionId(),
                        transaction.getTransactionAmount(),
                        transaction.getCurrency())
        ).print();

            transactionStream.sinkTo(sink);

            env.execute("Simple Transaction Cource comsumption Usecase");
        }

}
