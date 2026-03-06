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
 * Custom exception thrown by KafkaMessageSampler implementations when
 * message generation fails.
 */
public class KafkaSamplerException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Constructs a new KafkaSamplerException with the specified detail message.
     *
     * @param message the detail message
     */
    public KafkaSamplerException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new KafkaSamplerException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public KafkaSamplerException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new KafkaSamplerException with the specified cause.
     *
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public KafkaSamplerException(Throwable cause) {
        super(cause);
    }
}
