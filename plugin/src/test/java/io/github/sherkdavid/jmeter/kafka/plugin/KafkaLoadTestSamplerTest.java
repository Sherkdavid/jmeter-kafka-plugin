package io.github.sherkdavid.jmeter.kafka.plugin;

import org.junit.Before;
import org.junit.Test;
import io.github.sherkdavid.jmeter.kafka.plugin.sampler.KafkaLoadTestSampler;

import static org.junit.Assert.*;

public class KafkaLoadTestSamplerTest {
    
    private KafkaLoadTestSampler sampler;
    
    @Before
    public void setUp() {
        sampler = new KafkaLoadTestSampler();
    }
    
    @Test
    public void testDefaultProperties() {
        assertEquals("localhost:9092", sampler.getBootstrapServers());
        assertEquals("test-topic", sampler.getTopic());
        assertEquals("16384", sampler.getBatchSize());
        assertEquals("10", sampler.getLingerMs());
        assertEquals("1", sampler.getAcks());
        assertEquals("none", sampler.getCompressionType());
        assertEquals("PLAINTEXT", sampler.getSecurityProtocol());
        assertEquals("", sampler.getSaslJaasConfig());
    }
    
    @Test
    public void testPropertySetters() {
        sampler.setBootstrapServers("kafka:9092");
        sampler.setTopic("my-topic");
        sampler.setProducerClass("com.example.MyProducer");
        sampler.setMessageKey("key1");
        sampler.setBatchSize("32768");
        sampler.setLingerMs("20");
        sampler.setAcks("all");
        sampler.setCompressionType("gzip");
        sampler.setSecurityProtocol("SASL_SSL");
        sampler.setSaslJaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";");
        
        assertEquals("kafka:9092", sampler.getBootstrapServers());
        assertEquals("my-topic", sampler.getTopic());
        assertEquals("com.example.MyProducer", sampler.getProducerClass());
        assertEquals("key1", sampler.getMessageKey());
        assertEquals("32768", sampler.getBatchSize());
        assertEquals("20", sampler.getLingerMs());
        assertEquals("all", sampler.getAcks());
        assertEquals("gzip", sampler.getCompressionType());
        assertEquals("SASL_SSL", sampler.getSecurityProtocol());
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";", sampler.getSaslJaasConfig());
    }
}
