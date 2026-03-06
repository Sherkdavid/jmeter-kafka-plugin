# JMeter Kafka Plugin - Detailed Usage Guide

## Table of Contents

1. [Creating Your Producer](#creating-your-producer)
2. [Module Layout](#module-layout)
3. [Building the Plugin with Gradle](#building-the-plugin-with-gradle)
4. [Publishing Artifacts](#publishing-artifacts)
5. [Building Your Producer JAR](#building-your-producer-jar)
6. [Installing Your Producer](#installing-your-producer)
7. [Configuring the Sampler](#configuring-the-sampler)
8. [Examples](#examples)
9. [Troubleshooting](#troubleshooting)

## Module Layout

- `api` module: contains `KafkaMessageProducer` and `KafkaSamplerException`.
- `plugin` module: contains the JMeter sampler/GUI and runtime integration.

Artifact coordinates:

- `io.github.sherkdavid.jmeter.kafka:jmeter-kafka-sampler-api:1.0.0`
- `io.github.sherkdavid.jmeter.kafka:jmeter-kafka-sampler:1.0.0`

## Creating Your Producer

Your class must implement `io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer` and return a `byte[]` payload.

### Minimal Example

```java
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;
import java.nio.charset.StandardCharsets;

public class SimpleProducer implements KafkaMessageProducer {

    @Override
    public byte[] produceMessage() throws KafkaSamplerException {
        String message = "Hello Kafka at " + System.currentTimeMillis();
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
```

### With Error Handling

```java
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RobustProducer implements KafkaMessageProducer {

    @Override
    public byte[] produceMessage() throws KafkaSamplerException {
        try {
            String message = generateMessage();
            return message.getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new KafkaSamplerException("IO error generating message", e);
        } catch (Exception e) {
            throw new KafkaSamplerException("Unexpected error: " + e.getMessage(), e);
        }
    }

    private String generateMessage() throws IOException {
        return "message";
    }
}
```

## Building the Plugin with Gradle

Use the Gradle wrapper included in this repository.

### Windows

```powershell
.\gradlew.bat clean build
```

### Linux/macOS

```bash
./gradlew clean build
```

Useful tasks:

```bash
./gradlew test
./gradlew javadoc
./gradlew :plugin:shadowJar
```

The plugin runtime JAR is generated under `plugin/build/libs/`.

## Publishing Artifacts

Publish both modules to local Maven:

```bash
./gradlew publishToMavenLocal
```

This makes both artifacts available to downstream projects from `~/.m2/repository`.

Publish to Sonatype (Maven Central staging):

```bash
./gradlew publish
```

Convenience scripts:

- Windows PowerShell: `./release.ps1` (or `./release.ps1 -Local`)
- Linux/macOS: `./release.sh` (or `./release.sh --local`)

Required environment variables:

- `OSSRH_USERNAME`
- `OSSRH_PASSWORD`
- `SIGNING_KEY` (ASCII-armored private key)
- `SIGNING_PASSWORD`

Publishing is already configured for `s01.oss.sonatype.org`.

## Building Your Producer JAR

### Option 1: Compile a standalone class against API JAR

```bash
javac -cp api/build/libs/jmeter-kafka-sampler-api-1.0.0.jar SimpleProducer.java
jar -cf my-producer.jar SimpleProducer.class
```

### Option 2: Build your own Gradle/Maven project

Create your producer project and use the API artifact dependency.

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

## Installing Your Producer

Copy your producer JAR to JMeter classpath:

```bash
cp my-producer.jar "$JMETER_HOME/lib/"
```

You can also place it in:

```bash
cp my-producer.jar "$JMETER_HOME/lib/ext/"
```

Then restart JMeter.

Install plugin runtime JAR in JMeter as well:

```bash
cp plugin/build/libs/jmeter-kafka-sampler-1.0.0.jar "$JMETER_HOME/lib/ext/"
```

## Configuring the Sampler

### GUI Configuration

1. Open JMeter.
2. Add **Kafka Load Test Sampler** to your test plan.
3. Fill these fields:
   - **Bootstrap Servers**: for example `localhost:9092`
   - **Topic**: for example `my-test-topic`
   - **Producer Class**: for example `com.example.SimpleProducer`

### Optional Tuning

- **Message Key**: route messages by key
- **Batch Size**: producer batch size in bytes
- **Linger (ms)**: batching delay
- **Acks**: `0`, `1`, or `all`
- **Compression**: `none`, `gzip`, `snappy`, `lz4`, `zstd`

## Examples

### Example 1: Random Test Data

```java
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class RandomDataProducer implements KafkaMessageProducer {

    private final Random random = new Random();

    @Override
    public byte[] produceMessage() throws KafkaSamplerException {
        try {
            String userId = "user_" + random.nextInt(10000);
            String action = getRandomAction();
            long timestamp = System.currentTimeMillis();
            String message = userId + "|" + action + "|" + timestamp;
            return message.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new KafkaSamplerException("Failed to generate message", e);
        }
    }

    private String getRandomAction() {
        String[] actions = {"LOGIN", "LOGOUT", "CLICK", "PURCHASE", "VIEW"};
        return actions[random.nextInt(actions.length)];
    }
}
```

### Example 2: JSON Message Producer

```java
import com.google.gson.Gson;
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class JsonMessageProducer implements KafkaMessageProducer {

    private final Gson gson = new Gson();
    private int messageId = 0;

    @Override
    public byte[] produceMessage() throws KafkaSamplerException {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("id", ++messageId);
            message.put("timestamp", System.currentTimeMillis());
            message.put("type", "test_event");
            message.put("value", Math.random() * 100);
            String json = gson.toJson(message);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new KafkaSamplerException("Failed to create JSON message", e);
        }
    }
}
```

### Example 3: CSV Data-Driven Producer

```java
import io.github.sherkdavid.jmeter.kafka.api.KafkaMessageProducer;
import io.github.sherkdavid.jmeter.kafka.api.KafkaSamplerException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CsvDataProducer implements KafkaMessageProducer {

    private final List<String> csvLines;
    private int currentIndex = 0;

    public CsvDataProducer() throws KafkaSamplerException {
        try {
            csvLines = Files.readAllLines(Paths.get("/path/to/data.csv"));
        } catch (Exception e) {
            throw new KafkaSamplerException("Failed to load CSV file", e);
        }
    }

    @Override
    public byte[] produceMessage() throws KafkaSamplerException {
        if (csvLines.isEmpty()) {
            throw new KafkaSamplerException("No CSV data available");
        }

        String line = csvLines.get(currentIndex % csvLines.size());
        currentIndex++;
        return line.getBytes(StandardCharsets.UTF_8);
    }
}
```

## Troubleshooting

### ClassNotFoundException

- Ensure your producer JAR is in `$JMETER_HOME/lib/` or `$JMETER_HOME/lib/ext/`.
- Verify the fully qualified class name in sampler config.
- Restart JMeter after adding new JARs.

### Message generation errors

- Check JMeter log: `$JMETER_HOME/logs/jmeter.log`
- Add logging inside your producer implementation.
- Ensure all producer dependencies are available in JMeter classpath.

### Kafka connectivity issues

- Verify bootstrap servers and topic values.
- Ensure Kafka is running and reachable.
- Test connectivity to broker port (for example `9092`).
