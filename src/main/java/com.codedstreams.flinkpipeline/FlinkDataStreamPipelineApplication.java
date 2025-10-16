package com.codedstreams.flinkpipeline;

import com.codedstream.transfruad.library.schema.CardTransaction;
import com.codedstream.transfruad.library.schema.ProcessedTransaction;
import com.codedstream.transfruad.library.schema.ProcessedTransactionFeatures;
import com.codedstream.transfruad.library.schema.ProcessingStatus;
import com.codedstream.transfruad.library.schema.FeatureVector;
import com.codedstream.transfruad.library.schema.ModelMetadata;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FlinkDataStreamPipelineApplication {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDataStreamPipelineApplication.class);
    private static final double FRAUD_THRESHOLD = 0.55;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for exactly-once processing
        env.enableCheckpointing(10000);

        // Set parallelism based on your cluster
        env.setParallelism(2);

        String bootstrapServers = "kafka-01:29092";
        String schemaRegistryUrl = "http://schemaregistry0-01:8081";
        String inputTopic = "financial-transactions";
        String outputTopic = "fraud-detection-results";
        String featuresTopic = "processed-transaction-feature";
        String consumerGroupId = "fraud-detection-consumer-group";

        LOG.info("Initializing Real-time Fraud Detection Pipeline");

        // Kafka Source
        KafkaSource<CardTransaction> source = KafkaSource.<CardTransaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                CardTransaction.class,
                                schemaRegistryUrl
                        )
                )
                .build();

        // Kafka Sink for fraud results
        KafkaSink<ProcessedTransaction> fraudSink = KafkaSink.<ProcessedTransaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(
                                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                ProcessedTransaction.class,
                                                outputTopic + "-value",
                                                schemaRegistryUrl
                                        )
                                )
                                .build()
                )
                .build();

        // Kafka Sink for processed features (new sink)
        KafkaSink<ProcessedTransactionFeatures> featuresSink = KafkaSink.<ProcessedTransactionFeatures>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(featuresTopic)
                                .setValueSerializationSchema(
                                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                ProcessedTransactionFeatures.class,
                                                featuresTopic + "-value",
                                                schemaRegistryUrl
                                        )
                                )
                                .build()
                )
                .build();

        // Create processing pipeline
        DataStream<CardTransaction> transactionStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Transaction Source"
        );

        // Fraud detection pipeline
        SingleOutputStreamOperator<ProcessedTransaction> fraudDetectionStream = transactionStream
                .map(new FraudDetectionFunction())
                .name("Fraud Detection Model")
                .setParallelism(2);

        // Create features stream for model training
        SingleOutputStreamOperator<ProcessedTransactionFeatures> featuresStream = transactionStream
                .map(new FeaturesExtractionFunction())
                .name("Features Extraction")
                .setParallelism(2);

        // Monitoring and logging
        fraudDetectionStream
                .map(new FraudResultLogger())
                .name("Fraud Result Logger")
                .setParallelism(1);

        // Send fraud results to fraud-detection-results topic
        fraudDetectionStream.sinkTo(fraudSink)
                .name("Fraud Results Sink");

        // Send processed features to processed-transactions topic for future training
        featuresStream.sinkTo(featuresSink)
                .name("Features Sink");

        LOG.info("Starting Fraud Detection Pipeline Execution");
        LOG.info("Output topics: {} (fraud results), {} (features for training)", outputTopic, featuresTopic);
        env.execute("Real-time Fraud Detection Pipeline");
    }

    /**
     * Function to extract features and create ProcessedTransactionFeatures
     */
    public static class FeaturesExtractionFunction extends RichMapFunction<CardTransaction, ProcessedTransactionFeatures> {
        private static final long serialVersionUID = 1L;

        @Override
        public ProcessedTransactionFeatures map(CardTransaction transaction) throws Exception {
            try {
                // Extract features using the same FeatureExtractor
                Map<String, Float> features = new FeatureExtractor(transaction).extractAllFeatures();

                // Build ProcessedTransactionFeatures with all required fields
                ProcessedTransactionFeatures featuresRecord = ProcessedTransactionFeatures.newBuilder()
                        .setTransactionId(transaction.getTransactionId().toString())
                        .setTimestamp(transaction.getTransactionTimestamp())
                        .setMerchant(features.get("merchant"))
                        .setCategory(features.get("category"))
                        .setAmt(features.get("amt"))
                        .setFirst(features.get("first"))
                        .setLast(features.get("last"))
                        .setGender(features.get("gender"))
                        .setCity(features.get("city"))
                        .setState(features.get("state"))
                        .setZip(features.get("zip"))
                        .setLat(features.get("lat"))
                        .setLong$(features.get("long"))
                        .setCityPop(features.get("city_pop"))
                        .setJob(features.get("job"))
                        .setMerchLat(features.get("merch_lat"))
                        .setMerchLong(features.get("merch_long"))
                        .setHour(features.get("hour"))
                        .setDayofweek(features.get("dayofweek"))
                        .setMonth(features.get("month"))
                        .setDay(features.get("day"))
                        .setDistToMerchantKm(features.get("dist_to_merchant_km"))
                        .setIsFraud(false)
                        .build();

                return featuresRecord;

            } catch (Exception e) {
                LOG.error("Error extracting features for transaction: {}", transaction.getTransactionId(), e);
                // Return a minimal record with error indication
                return ProcessedTransactionFeatures.newBuilder()
                        .setTransactionId(transaction.getTransactionId().toString())
                        .setTimestamp(transaction.getTransactionTimestamp())
                        .setMerchant(0.0f)
                        .setCategory(0.0f)
                        .setAmt(0.0f)
                        .setFirst(0.0f)
                        .setLast(0.0f)
                        .setGender(0.0f)
                        .setCity(0.0f)
                        .setState(0.0f)
                        .setZip(0.0f)
                        .setLat(0.0f)
                        .setLong$(0.0f)
                        .setCityPop(0.0f)
                        .setJob(0.0f)
                        .setMerchLat(0.0f)
                        .setMerchLong(0.0f)
                        .setHour(0.0f)
                        .setDayofweek(0.0f)
                        .setMonth(0.0f)
                        .setDay(0.0f)
                        .setDistToMerchantKm(0.0f)
                        .setIsFraud(false)
                        .build();
            }
        }
    }

    /**
     * Main fraud detection function with rule-based approach
     */
    public static class FraudDetectionFunction extends RichMapFunction<CardTransaction, ProcessedTransaction> {
        private static final long serialVersionUID = 1L;

        private transient FraudDetectionEngine fraudEngine;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.fraudEngine = new FraudDetectionEngine();
            LOG.info("FraudDetectionFunction initialized with threshold: {}", FRAUD_THRESHOLD);
        }

        @Override
        public ProcessedTransaction map(CardTransaction transaction) throws Exception {
            long startTime = System.currentTimeMillis();

            try {
                // Extract features using ONLY data from this transaction
                Map<String, Float> features = new FeatureExtractor(transaction).extractAllFeatures();

                // Perform fraud prediction using rule-based engine
                FraudPrediction prediction = fraudEngine.predict(features);

                // Create output with fraud detection results
                ProcessedTransaction result = buildProcessedTransaction(
                        transaction, prediction, System.currentTimeMillis() - startTime
                );

                return result;

            } catch (Exception e) {
                LOG.error("Error processing transaction: {}", transaction.getTransactionId(), e);
                return buildErrorResult(transaction, e.getMessage());
            }
        }

        private ProcessedTransaction buildProcessedTransaction(CardTransaction transaction,
                                                               FraudPrediction prediction, long processingTimeMs) {

            // Create feature vector for audit trail
            FeatureVector featureVector = FeatureVector.newBuilder()
                    .setAmountDeviation((float) prediction.getBehavioralAnomalyScore())
                    .setVelocity1h(0.0f)
                    .setVelocity24h(0.0f)
                    .setDistanceFromHome((float) prediction.getDeviceRisk())
                    .setTimeOfDayRisk((float) prediction.getTimeBasedRisk())
                    .setMerchantRiskScore((float) prediction.getMerchantRisk())
                    .setDeviceRiskScore((float) prediction.getDeviceRisk())
                    .setBehavioralAnomalyScore((float) prediction.getBehavioralAnomalyScore())
                    .build();

            // Create model metadata
            ModelMetadata modelMetadata = ModelMetadata.newBuilder()
                    .setModelVersion("1.0.0")
                    .setModelType("rule-based")
                    .setInferenceTimeMs(processingTimeMs)
                    .setConfidence((float) prediction.getConfidence())
                    .build();

            // Determine processing status
            ProcessingStatus status = prediction.isFraud() ? ProcessingStatus.REJECTED :
                    prediction.getProbability() > 0.3 ? ProcessingStatus.FLAGGED_FOR_REVIEW :
                            ProcessingStatus.APPROVED;

            return ProcessedTransaction.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setProcessingTimestamp(System.currentTimeMillis())
                    .setFraudProbability(prediction.getProbability())
                    .setStatus(status)
                    .setFeatureVector(featureVector)
                    .setWindowedFeatures(null)
                    .setModelMetadata(modelMetadata)
                    .build();
        }

        private ProcessedTransaction buildErrorResult(CardTransaction transaction, String error) {
            return ProcessedTransaction.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setProcessingTimestamp(System.currentTimeMillis())
                    .setFraudProbability(0.0)
                    .setStatus(ProcessingStatus.PENDING)
                    .setFeatureVector(FeatureVector.newBuilder().build())
                    .setModelMetadata(ModelMetadata.newBuilder()
                            .setModelVersion("1.0.0")
                            .setModelType("error")
                            .setInferenceTimeMs(0L)
                            .setConfidence(0.0f)
                            .build())
                    .build();
        }
    }

    /**
     * Rule-based Fraud Detection Engine (No XGBoost dependency)
     */
    public static class FraudDetectionEngine implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Random random = new Random();

        public FraudPrediction predict(Map<String, Float> features) {
            // Calculate base fraud probability using rule-based approach
            double baseProbability = calculateBaseFraudProbability(features);

            // Add some randomness to simulate model uncertainty
            double noise = (random.nextDouble() - 0.5) * 0.1;
            double fraudProbability = Math.max(0.0, Math.min(1.0, baseProbability + noise));

            // Calculate risk scores based on features
            double timeRisk = calculateTimeBasedRisk(features);
            double merchantRisk = calculateMerchantRisk(features);
            double deviceRisk = calculateDeviceRisk(features);
            double behavioralAnomaly = calculateBehavioralAnomaly(features);

            boolean isFraud = fraudProbability > FRAUD_THRESHOLD;
            double confidence = calculatePredictionConfidence(fraudProbability);

            return new FraudPrediction(fraudProbability, isFraud, confidence,
                    timeRisk, merchantRisk, deviceRisk, behavioralAnomaly);
        }

        private double calculateBaseFraudProbability(Map<String, Float> features) {
            double probability = 0.05; // Base fraud rate

            // Rule 1: High amount transactions
            Float amount = features.get("amt");
            if (amount != null && amount > 1000) {
                probability += 0.3;
            } else if (amount != null && amount > 500) {
                probability += 0.15;
            }

            // Rule 2: Unusual hours (10 PM - 6 AM)
            Float hour = features.get("hour");
            if (hour != null && (hour >= 22 || hour <= 6)) {
                probability += 0.2;
            }

            // Rule 3: Online transactions without card present
            Float isOnline = features.get("is_online");
            Float isCardPresent = features.get("is_card_present");
            if (isOnline != null && isOnline > 0.5 &&
                    isCardPresent != null && isCardPresent < 0.5) {
                probability += 0.25;
            }

            // Rule 4: High-risk merchant categories
            Float merchantRisk = features.get("merchant_risk");
            if (merchantRisk != null && merchantRisk > 0.7) {
                probability += 0.2;
            }

            // Rule 5: High-risk device types
            Float deviceType = features.get("device_type");
            if (deviceType != null && deviceType > 0.6) {
                probability += 0.15;
            }

            // Rule 6: High-risk IP addresses
            Float ipRisk = features.get("ip_risk");
            if (ipRisk != null && ipRisk > 0.7) {
                probability += 0.15;
            }

            return Math.min(probability, 0.95); // Cap at 95%
        }

        private double calculateTimeBasedRisk(Map<String, Float> features) {
            Float hour = features.get("hour");
            if (hour == null) return 0.0;
            return (hour >= 22 || hour <= 6) ? 0.8 : 0.2;
        }

        private double calculateMerchantRisk(Map<String, Float> features) {
            Float merchantRisk = features.get("merchant_risk");
            return merchantRisk != null ? merchantRisk : 0.3;
        }

        private double calculateDeviceRisk(Map<String, Float> features) {
            Float deviceTypeRisk = features.get("device_type");
            Float ipRisk = features.get("ip_risk");
            double risk = 0.3;
            if (deviceTypeRisk != null) risk += deviceTypeRisk * 0.3;
            if (ipRisk != null) risk += ipRisk * 0.4;
            return Math.min(risk, 1.0);
        }

        private double calculateBehavioralAnomaly(Map<String, Float> features) {
            double anomalyScore = 0.0;
            Float amount = features.get("amt");
            Float isOnline = features.get("is_online");
            Float isCardPresent = features.get("is_card_present");
            Float hour = features.get("hour");

            if (amount != null && amount > 1000) anomalyScore += 0.3;
            if (isOnline != null && isOnline > 0.5 &&
                    isCardPresent != null && isCardPresent < 0.5) {
                anomalyScore += 0.3;
            }
            if (hour != null && (hour >= 22 || hour <= 6) && amount != null && amount > 500) {
                anomalyScore += 0.2;
            }
            return Math.min(anomalyScore, 1.0);
        }

        private double calculatePredictionConfidence(double probability) {
            // Confidence is highest when probability is near 0 or 1
            return 1.0 - Math.abs(probability - 0.5) * 2;
        }
    }

    /**
     * Feature Extractor (same as before)
     */
    public static class FeatureExtractor {
        private final CardTransaction transaction;

        public FeatureExtractor(CardTransaction transaction) {
            this.transaction = transaction;
        }

        public Map<String, Float> extractAllFeatures() {
            Map<String, Float> features = new HashMap<>();

            // Basic transaction features
            features.put("amt", (float) transaction.getTransactionAmount());
            features.put("amt_log", (float) Math.log(transaction.getTransactionAmount() + 1));

            // Time-based features
            extractTimeFeatures(features);

            // Location features
            extractLocationFeatures(features);

            // Merchant features
            extractMerchantFeatures(features);

            // Device and transaction type features
            extractDeviceAndTypeFeatures(features);

            // Fill missing features with defaults
            fillMissingFeatures(features);

            return features;
        }

        private void extractTimeFeatures(Map<String, Float> features) {
            Instant instant = Instant.ofEpochMilli(transaction.getTransactionTimestamp());
            var zonedDateTime = instant.atZone(ZoneId.systemDefault());

            features.put("hour", (float) zonedDateTime.getHour());
            features.put("dayofweek", (float) zonedDateTime.getDayOfWeek().getValue());
            features.put("month", (float) zonedDateTime.getMonthValue());
            features.put("day", (float) zonedDateTime.getDayOfMonth());
        }

        private void extractLocationFeatures(Map<String, Float> features) {
            if (transaction.getMerchantLocation() != null) {
                features.put("lat", (float) transaction.getMerchantLocation().getLatitude());
                features.put("long", (float) transaction.getMerchantLocation().getLongitude());

                float cityPop = getCityPopulation(transaction.getMerchantLocation().getCity().toString());
                features.put("city_pop", cityPop);

                features.put("merch_lat", (float) transaction.getMerchantLocation().getLatitude());
                features.put("merch_long", (float) transaction.getMerchantLocation().getLongitude());
                features.put("dist_to_merchant_km", 0.0f);

            } else {
                features.put("lat", 0.0f);
                features.put("long", 0.0f);
                features.put("city_pop", 1000000.0f);
                features.put("merch_lat", 0.0f);
                features.put("merch_long", 0.0f);
                features.put("dist_to_merchant_km", 0.0f);
            }
        }

        private void extractMerchantFeatures(Map<String, Float> features) {
            features.put("merchant", encodeStringToFloat(transaction.getMerchantId().toString()));
            features.put("category", encodeMerchantCategory(transaction.getMerchantCategory().toString()));
            features.put("merchant_risk", calculateMerchantRisk(
                    transaction.getMerchantCategory().toString(),
                    transaction.getMerchantName().toString()
            ));
        }

        private void extractDeviceAndTypeFeatures(Map<String, Float> features) {
            String transactionType = transaction.getTransactionType().toString();
            features.put("is_online", "ONLINE".equals(transactionType) ? 1.0f : 0.0f);
            features.put("is_pos", "POS".equals(transactionType) ? 1.0f : 0.0f);
            features.put("is_card_present", transaction.getIsCardPresent() ? 1.0f : 0.0f);

            if (transaction.getDeviceInfo() != null) {
                var deviceInfo = transaction.getDeviceInfo();
                features.put("device_type", encodeDeviceType(deviceInfo.getDeviceType().toString()));
                features.put("ip_risk", calculateIPRisk(deviceInfo.getIpAddress().toString()));
            } else {
                features.put("device_type", 0.0f);
                features.put("ip_risk", 0.5f);
            }
        }

        private float getCityPopulation(String city) {
            Map<String, Float> cityPopulations = Map.of(
                    "New York", 8336817.0f,
                    "Los Angeles", 3970000.0f,
                    "Houston", 2300000.0f,
                    "Chicago", 2690000.0f,
                    "Phoenix", 1660000.0f,
                    "Philadelphia", 1580000.0f
            );
            return cityPopulations.getOrDefault(city, 1000000.0f);
        }

        private float encodeStringToFloat(String value) {
            return (float) (Math.abs(value.hashCode()) % 1000) / 1000.0f;
        }

        private float encodeMerchantCategory(String category) {
            Map<String, Float> riskMap = Map.of(
                    "RETAIL", 0.2f,
                    "FOOD", 0.3f,
                    "GAS", 0.7f,
                    "ONLINE", 0.6f,
                    "ELECTRONICS", 0.8f,
                    "JEWELRY", 0.9f
            );
            return riskMap.getOrDefault(category.toUpperCase(), 0.5f);
        }

        private float encodeDeviceType(String deviceType) {
            Map<String, Float> deviceRiskMap = Map.of(
                    "MOBILE", 0.3f,
                    "TABLET", 0.4f,
                    "DESKTOP", 0.2f,
                    "UNKNOWN", 0.7f
            );
            return deviceRiskMap.getOrDefault(deviceType.toUpperCase(), 0.5f);
        }

        private float calculateMerchantRisk(String category, String merchantName) {
            float risk = encodeMerchantCategory(category);
            if (merchantName.matches(".*\\d+$")) risk += 0.1f;
            if (merchantName.toLowerCase().contains("gas")) risk += 0.2f;
            if (merchantName.toLowerCase().contains("pawn")) risk += 0.4f;
            return Math.min(risk, 1.0f);
        }

        private float calculateIPRisk(String ipAddress) {
            if (ipAddress.startsWith("192.168.") || ipAddress.startsWith("10.")) {
                return 0.8f;
            }
            return 0.3f;
        }

        private void fillMissingFeatures(Map<String, Float> features) {
            // Demographic features (placeholders)
            features.put("first", 0.0f);
            features.put("last", 0.0f);
            features.put("gender", 0.0f);
            features.put("city", 0.0f);
            features.put("state", 0.0f);
            features.put("zip", 0.0f);
            features.put("job", 0.0f);
        }
    }

    /**
     * Fraud Prediction Result
     */
    public static class FraudPrediction implements Serializable {
        private final double probability;
        private final boolean isFraud;
        private final double confidence;
        private final double timeBasedRisk;
        private final double merchantRisk;
        private final double deviceRisk;
        private final double behavioralAnomalyScore;

        public FraudPrediction(double probability, boolean isFraud, double confidence,
                               double timeBasedRisk, double merchantRisk, double deviceRisk,
                               double behavioralAnomalyScore) {
            this.probability = probability;
            this.isFraud = isFraud;
            this.confidence = confidence;
            this.timeBasedRisk = timeBasedRisk;
            this.merchantRisk = merchantRisk;
            this.deviceRisk = deviceRisk;
            this.behavioralAnomalyScore = behavioralAnomalyScore;
        }

        public double getProbability() { return probability; }
        public boolean isFraud() { return isFraud; }
        public double getConfidence() { return confidence; }
        public double getTimeBasedRisk() { return timeBasedRisk; }
        public double getMerchantRisk() { return merchantRisk; }
        public double getDeviceRisk() { return deviceRisk; }
        public double getBehavioralAnomalyScore() { return behavioralAnomalyScore; }
    }

    /**
     * Fraud Result Logger for monitoring
     */
    public static class FraudResultLogger extends RichMapFunction<ProcessedTransaction, ProcessedTransaction> {
        private static final long serialVersionUID = 1L;
        private transient Logger logger;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.logger = LoggerFactory.getLogger("FraudDetectionLogger");
        }

        @Override
        public ProcessedTransaction map(ProcessedTransaction result) throws Exception {
            String fraudStatus = result.getFraudProbability() > FRAUD_THRESHOLD ? "FRAUD" : "LEGIT";

            logger.info("Fraud Detection - Transaction: {}, Probability: {:.4f}, Status: {}, Final: {}",
                    result.getTransactionId(),
                    result.getFraudProbability(),
                    result.getStatus(),
                    fraudStatus);

            return result;
        }
    }
}