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
package io.github.sherkdavid.jmeter.kafka.api;

/**
 * Interface for generating Kafka message payloads in JMeter load tests.
 * 
 * Implementations of this interface are responsible for creating the byte array
 * payload that will be sent to the Kafka topic. This is the main contract that
 * users of this library must implement.
 * 
 * <p>Example implementation:</p>
 * <pre>
 * public class MyCustomProducer implements KafkaMessageProducer {
 *
 *     private int counter = 0;
 *
 *     &#64;Override
 *     public byte[] produceMessage() throws KafkaSamplerException {
 *         String message = "Message " + (counter++) + " at " + System.currentTimeMillis();
 *         return message.getBytes(StandardCharsets.UTF_8);
 *     }
 * }
 * </pre>
 */
public interface KafkaMessageProducer {
    
    /**
     * Generates a message payload for Kafka.
     * 
     * This method is called for each message that needs to be sent to Kafka.
     * Implementations should generate the actual message payload that will be
     * produced to the Kafka topic as configured in the JMeter sampler.
     *
     * @return the message payload as a byte array. Must not be null.
     * @throws KafkaSamplerException if message generation fails for any reason
     */
    byte[] produceMessage() throws KafkaSamplerException;
}
