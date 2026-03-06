# JMeter Kafka Load Test Plugin

JMeter plugin for producing Kafka messages during load tests, with custom payload generation via Java.

## Overview

This plugin adds a **Kafka Load Test Sampler** to JMeter. You provide a class that implements `KafkaMessageProducer`, and the sampler invokes it for each message.

## Features

- Custom message generation through a simple interface
- Configurable Kafka producer settings (acks, batch size, linger, compression)
- Security configuration support (`security.protocol`, `sasl.jaas.config`)
- Optional message key support
- Multi-module build: separate API and JMeter plugin artifacts
- Built with Gradle (wrapper included for Windows/Linux/macOS)

## Modules

- `api` - public interfaces/exceptions used by producer implementations
- `plugin` - JMeter sampler + GUI implementation

## Published Artifacts

- `io.github.sherkdavid.jmeter.kafka:jmeter-kafka-sampler-api:1.0.0` (compile-time dependency for producer projects)
- `io.github.sherkdavid.jmeter.kafka:jmeter-kafka-sampler:1.0.0` (runtime plugin JAR for JMeter)

## Requirements

- Java 11+
- JMeter 5.6+

## Build (Gradle)

Use the included Gradle wrapper:

### Windows

```powershell
.\gradlew.bat clean build
```

### Linux/macOS

```bash
./gradlew clean build
```

To run only Javadocs:

```bash
./gradlew javadoc
```

Publish to local Maven repository:

```bash
./gradlew publishToMavenLocal
```

Publish to Sonatype (Maven Central staging):

```bash
./gradlew publish
```

Convenience scripts:

- Windows PowerShell: `./release.ps1` (or `./release.ps1 -Local`)
- Linux/macOS: `./release.sh` (or `./release.sh --local`)

Required environment variables for remote publish/signing:

- `OSSRH_USERNAME`
- `OSSRH_PASSWORD`
- `SIGNING_KEY` (ASCII-armored private key)
- `SIGNING_PASSWORD`

The build is preconfigured for `s01.oss.sonatype.org` (snapshot vs release URL chosen by version suffix).

## Install in JMeter

After build, copy the plugin runtime JAR from `plugin/build/libs` into JMeter:

```bash
cp plugin/build/libs/jmeter-kafka-sampler-1.0.0.jar "$JMETER_HOME/lib/ext/"
```

Then restart JMeter.

## Quick Start

### 1) Implement `KafkaMessageProducer`

```java
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;
import java.nio.charset.StandardCharsets;

public class MyCustomProducer implements KafkaMessageProducer {

    private int counter = 0;

    @Override
    public byte[] produceMessage() throws KafkaSamplerException {
        try {
            String message = "Message #" + (++counter) + " at " + System.currentTimeMillis();
            return message.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new KafkaSamplerException("Failed to generate message", e);
        }
    }
}
```

### 2) Package your producer class

Use the API artifact as your compile dependency in your producer project.

Gradle:

```gradle
dependencies {
    implementation 'io.github.sherkdavid.jmeter.kafka:jmeter-kafka-sampler-api:1.0.0'
}
```

Maven:

```xml
<dependency>
    <groupId>io.github.sherkdavid.jmeter.kafka</groupId>
    <artifactId>jmeter-kafka-sampler-api</artifactId>
    <version>1.0.0</version>
</dependency>
```

Build your producer JAR and place it in JMeter classpath, for example `JMETER_HOME/lib`.

### 3) Configure sampler in JMeter

Add **Kafka Load Test Sampler** and set:

- Bootstrap Servers (for example `localhost:9092`)
- Topic (for example `my-topic`)
- Producer Class (fully qualified class name, for example `com.example.MyCustomProducer`)

## Sampler Configuration

### Required

| Property | Description | Example |
|---|---|---|
| Bootstrap Servers | Kafka broker addresses | `localhost:9092` |
| Topic | Target Kafka topic | `my-topic` |
| Producer Class | FQCN implementing `KafkaMessageProducer` | `com.example.MyCustomProducer` |

### Optional

| Property | Description | Default |
|---|---|---|
| Message Key | Message key for partitioning/routing | empty |
| Security Protocol | Kafka security protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`) | `PLAINTEXT` |
| SASL JAAS Config | JAAS login config passed as `sasl.jaas.config` | empty |
| Batch Size | Producer batch size (bytes) | `16384` |
| Linger (ms) | Producer linger time | `10` |
| Acks | Required acknowledgments (`0`, `1`, `all`) | `1` |
| Compression | `none`, `gzip`, `snappy`, `lz4`, `zstd` | `none` |

## `KafkaMessageProducer` Contract

```java
package io.github.sherkdavid.jmeter.kafka.api;

public interface KafkaMessageProducer {
    byte[] produceMessage() throws KafkaSamplerException;
}
```

## Useful Gradle Tasks

- `./gradlew clean build` - compile, test, package both modules
- `./gradlew test` - run tests
- `./gradlew javadoc` - generate API docs
- `./gradlew :plugin:shadowJar` - build shaded plugin runtime JAR
- `./gradlew publishToMavenLocal` - publish `api` and `plugin` artifacts locally

## Troubleshooting

- **Class not found**: ensure your producer JAR is in JMeter `lib` or `lib/ext`, then restart JMeter.
- **Connection errors**: verify Kafka bootstrap servers and topic.
- **Build issues**: run with `--stacktrace` (for example `./gradlew javadoc --stacktrace`).

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE).

## Support

For issues and feature requests, open a GitHub issue.