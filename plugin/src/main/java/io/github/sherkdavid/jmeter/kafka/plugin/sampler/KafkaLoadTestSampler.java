/**
 * Copyright 2026 David Murphy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.sherkdavid.jmeter.kafka.plugin.sampler;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;

import java.util.Properties;

/**
 * JMeter Sampler for sending messages to Kafka for load testing.
 * 
 * This sampler uses a user-provided KafkaMessageProducer implementation
 * to generate message payloads. Users must implement the KafkaMessageProducer
 * interface and provide the fully qualified class name in the sampler configuration.
 */
public class KafkaLoadTestSampler extends AbstractSampler implements ThreadListener, TestStateListener {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaLoadTestSampler.class);
    private static final long serialVersionUID = 1L;
    
    // Property names
    private static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String TOPIC = "kafka.topic";
    private static final String PRODUCER_CLASS = "kafka.producer.class";
    private static final String MESSAGE_KEY = "kafka.message.key";
    private static final String BATCH_SIZE = "kafka.batch.size";
    private static final String LINGER_MS = "kafka.linger.ms";
    private static final String ACKS = "kafka.acks";
    private static final String COMPRESSION_TYPE = "kafka.compression.type";
    private static final String SECURITY_PROTOCOL = "kafka.security.protocol";
    private static final String SASL_JAAS_CONFIG = "kafka.sasl.jaas.config";
    
    private transient KafkaProducer<String, byte[]> kafkaProducer;
    private transient KafkaMessageProducer messageProducer;
    private transient boolean initialized = false;
    
    /**
     * Default constructor
     */
    public KafkaLoadTestSampler() {
        super();
        initializeProperties();
    }
    
    /**
     * Initialize default properties
     */
    private void initializeProperties() {
        setProperty(BOOTSTRAP_SERVERS, "localhost:9092");
        setProperty(TOPIC, "test-topic");
        setProperty(PRODUCER_CLASS, "");
        setProperty(MESSAGE_KEY, "");
        setProperty(BATCH_SIZE, "16384");
        setProperty(LINGER_MS, "10");
        setProperty(ACKS, "1");
        setProperty(COMPRESSION_TYPE, "none");
        setProperty(SECURITY_PROTOCOL, "PLAINTEXT");
        setProperty(SASL_JAAS_CONFIG, "");
    }
    
    /**
     * Performs the Kafka message send operation
     */
    @Override
    public SampleResult sample(Entry entry) {
        SampleResult result = new SampleResult();
        result.setSampleLabel(getName());
        result.setContentType("application/octet-stream");
        result.setDataEncoding("UTF-8");
        
        try {
            result.sampleStart();
            
            // Initialize on first call
            if (!initialized) {
                initialize();
                initialized = true;
            }
            
            // Generate message using the user's producer implementation
            byte[] messagePayload = messageProducer.produceMessage();
            
            if (messagePayload == null) {
                throw new IllegalStateException(
                    "KafkaMessageProducer.produceMessage() returned null");
            }
            
            // Prepare the record
            String messageKey = getPropertyAsString(MESSAGE_KEY, "");
            ProducerRecord<String, byte[]> record = 
                new ProducerRecord<>(
                    getPropertyAsString(TOPIC),
                    messageKey.isEmpty() ? null : messageKey,
                    messagePayload
                );
            
            // Send synchronously for accurate timing
            kafkaProducer.send(record).get();
            
            result.sampleEnd();
            result.setSuccessful(true);
            result.setResponseCode("200");
            result.setResponseMessage("OK");
            
            logger.debug("Successfully sent message of size {} bytes to topic: {}",
                messagePayload.length, getPropertyAsString(TOPIC));
                
        } catch (KafkaSamplerException e) {
            result.sampleEnd();
            result.setSuccessful(false);
            result.setResponseCode("500");
            result.setResponseMessage("Producer error: " + e.getMessage());
            logger.error("KafkaMessageProducer failed", e);
            
        } catch (Exception e) {
            result.sampleEnd();
            result.setSuccessful(false);
            result.setResponseCode("500");
            result.setResponseMessage("Error: " + e.getMessage());
            logger.error("Failed to send Kafka message", e);
        }
        
        return result;
    }
    
    /**
     * Initializes the sampler on first use
     */
    private void initialize() throws Exception {
        // Create Kafka producer
        kafkaProducer = createKafkaProducer();
        logger.info("Kafka producer initialized for topic: {}", getPropertyAsString(TOPIC));
        
        // Instantiate user's message producer
        messageProducer = createMessageProducer();
        logger.info("Message producer initialized: {}", getPropertyAsString(PRODUCER_CLASS));
    }
    
    /**
     * Creates and configures the Kafka producer
     */
    private KafkaProducer<String, byte[]> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
            getPropertyAsString(BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
            StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
            ByteArraySerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 
            getPropertyAsString(BATCH_SIZE));
        props.put(ProducerConfig.LINGER_MS_CONFIG, 
            getPropertyAsString(LINGER_MS));
        props.put(ProducerConfig.ACKS_CONFIG, 
            getPropertyAsString(ACKS));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, 
            getPropertyAsString(COMPRESSION_TYPE));
        props.put("security.protocol",
            getPropertyAsString(SECURITY_PROTOCOL));

        String saslJaasConfig = getPropertyAsString(SASL_JAAS_CONFIG, "");
        if (saslJaasConfig != null && !saslJaasConfig.trim().isEmpty()) {
            props.put("sasl.jaas.config", saslJaasConfig);
        }
        
        logger.debug("Creating Kafka producer with bootstrap servers: {}", 
            getPropertyAsString(BOOTSTRAP_SERVERS));
        return new KafkaProducer<>(props);
    }
    
    /**
     * Creates an instance of the KafkaMessageProducer implementation
     */
    private KafkaMessageProducer createMessageProducer() throws Exception {
        String producerClassName = getPropertyAsString(PRODUCER_CLASS);
        
        if (producerClassName == null || producerClassName.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "KafkaMessageProducer class name not specified in sampler configuration");
        }
        
        try {
            Class<?> clazz = Class.forName(producerClassName);
            
            if (!KafkaMessageProducer.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(
                    "Class " + producerClassName + 
                    " does not implement KafkaMessageProducer interface");
            }
            
            return (KafkaMessageProducer) clazz.getDeclaredConstructor().newInstance();
            
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                "KafkaMessageProducer class not found: " + producerClassName, e);
        }
    }
    
    /**
     * Internal cleanup method
     */
    private synchronized void cleanup() {
        try {
            if (kafkaProducer != null) {
                kafkaProducer.flush();
                kafkaProducer.close();
                logger.info("Kafka producer closed");
            }
        } catch (Exception e) {
            logger.error("Error closing Kafka producer", e);
        }
        kafkaProducer = null;
        messageProducer = null;
        initialized = false;
    }

    @Override
    public void threadStarted() {
        // no-op: producer is lazily initialized in sample()
    }

    @Override
    public void threadFinished() {
        cleanup();
    }

    @Override
    public void testStarted() {
        // no-op
    }

    @Override
    public void testStarted(String host) {
        // no-op
    }

    @Override
    public void testEnded() {
        cleanup();
    }

    @Override
    public void testEnded(String host) {
        cleanup();
    }
    
    // ============ Property Accessors ============
    
    public void setBootstrapServers(String servers) {
        setProperty(BOOTSTRAP_SERVERS, servers);
    }
    
    public String getBootstrapServers() {
        return getPropertyAsString(BOOTSTRAP_SERVERS);
    }
    
    public void setTopic(String topic) {
        setProperty(TOPIC, topic);
    }
    
    public String getTopic() {
        return getPropertyAsString(TOPIC);
    }
    
    public void setProducerClass(String className) {
        setProperty(PRODUCER_CLASS, className);
    }
    
    public String getProducerClass() {
        return getPropertyAsString(PRODUCER_CLASS);
    }
    
    public void setMessageKey(String key) {
        setProperty(MESSAGE_KEY, key);
    }
    
    public String getMessageKey() {
        return getPropertyAsString(MESSAGE_KEY);
    }
    
    public void setBatchSize(String size) {
        setProperty(BATCH_SIZE, size);
    }
    
    public String getBatchSize() {
        return getPropertyAsString(BATCH_SIZE);
    }
    
    public void setLingerMs(String ms) {
        setProperty(LINGER_MS, ms);
    }
    
    public String getLingerMs() {
        return getPropertyAsString(LINGER_MS);
    }
    
    public void setAcks(String acks) {
        setProperty(ACKS, acks);
    }
    
    public String getAcks() {
        return getPropertyAsString(ACKS);
    }
    
    public void setCompressionType(String type) {
        setProperty(COMPRESSION_TYPE, type);
    }
    
    public String getCompressionType() {
        return getPropertyAsString(COMPRESSION_TYPE);
    }

    public void setSecurityProtocol(String securityProtocol) {
        setProperty(SECURITY_PROTOCOL, securityProtocol);
    }

    public String getSecurityProtocol() {
        return getPropertyAsString(SECURITY_PROTOCOL);
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        setProperty(SASL_JAAS_CONFIG, saslJaasConfig);
    }

    public String getSaslJaasConfig() {
        return getPropertyAsString(SASL_JAAS_CONFIG);
    }
}
