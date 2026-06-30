package com.collatz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Common configuration constants and helpers for Kafka.
 *
 * Learning moments:
 * - Bootstrap Servers: Tells the client how to connect to the Kafka cluster.
 * - Key/Value Serializers: Kafka only understands byte arrays. We must tell Kafka how to convert our Java
 *   keys (strings) and values (JSON strings) into bytes.
 * - Group ID: Identifies the consumer group. Kafka uses this to coordinate which worker reads from which partition.
 * - Auto Offset Reset: Tells Kafka where to start reading if a new consumer joins and there's no saved offset.
 */
public class KafkaConfig {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String WORK_TOPIC = "work";
    public static final String RESULTS_TOPIC = "results";

    /**
     * Creates standard properties for a Kafka Producer.
     */
    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Reliability / Performance tuning (Optional but good to know):
        // acks = all (wait for all replicas to acknowledge the write for maximum durability)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // retries = maximum (retry if broker connection is temporarily lost)
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        
        return props;
    }

    /**
     * Creates standard properties for a Kafka Consumer.
     *
     * @param groupId The consumer group ID to register under.
     */
    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Offset management:
        // 'earliest' means if no offset exists, start reading from the very beginning of the topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Disable auto-commit for reliable processing (Optional but standard):
        // We will manually commit offsets after we verify the chunk to avoid losing work!
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return props;
    }
}
