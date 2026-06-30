package com.collatz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Phase 3 & 4 — Worker node
 *
 * Workers pull tasks from the "work" topic, calculate the Collatz sequence length for every number
 * in the chunk, find the maximum chain length inside that chunk, and publish the results to the "results" topic.
 *
 * Learning moments:
 * - Kafka Consumer Loop: Polling for new messages in a while loop.
 * - Multi-role client: Being both a Consumer and a Producer in the same process.
 * - Deserialization: Converting a JSON string back into a Java record.
 * - Manual Offset Commit: Committing offsets only AFTER processing finishes so we don't lose tasks if we crash.
 * - Graceful Shutdown: Using a shutdown hook and consumer.wakeup() to exit the poll loop cleanly.
 */
public class Worker {

    private final String workerId;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Worker() {
        this.workerId = "worker-" + UUID.randomUUID().toString().substring(0, 5);
        this.mapper = new ObjectMapper();

        // Initialize Kafka Producer properties (for publishing results)
        this.producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());

        // Initialize Kafka Consumer properties (for consuming work chunks)
        // We use the consumer group name "collatz-workers-group"
        Properties consumerProps = KafkaConfig.getConsumerProperties("collatz-workers-group");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    public void run() {
        // Step 1: Subscribe the consumer to the WORK topic
        consumer.subscribe(Collections.singletonList(KafkaConfig.WORK_TOPIC));
        System.out.printf("[%s] Worker started. Subscribed to topic: %s%n", workerId, KafkaConfig.WORK_TOPIC);

        // Step 2: Register a shutdown hook to cleanly close Kafka clients when Ctrl+C is pressed
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down worker gracefully...");
            closed.set(true);
            // consumer.wakeup() is thread-safe and causes consumer.poll() to throw WakeupException
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (!closed.get()) {
                // Poll for new messages. Wait up to 100 milliseconds if no messages are present.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // TODO: Implement processing logic here.
                    // 1. Parse JSON value to Coordinator.ChunkRange object:
                    //    Coordinator.ChunkRange chunk = mapper.readValue(record.value(), Coordinator.ChunkRange.class);
                    //
                    // 2. Log that this worker started processing this chunk:
                    //    System.out.printf("[%s] Processing chunk %s: %s -> %s%n", workerId, chunk.chunkId(), chunk.start(), chunk.end());
                    //
                    // 3. Process the range: loop from chunk.start() to chunk.end() inclusive.
                    //    Find the number with the longest chain length in this chunk.
                    //    Hint: Use a loop like this:
                    //    long longestChain = 0;
                    //    BigInteger recordNumber = chunk.start();
                    //    for (BigInteger current = chunk.start(); current.compareTo(chunk.end()) <= 0; current = current.add(BigInteger.ONE)) {
                    //        long length = CollatzVerifier.calculateChainLength(current);
                    //        if (length > longestChain) {
                    //            longestChain = length;
                    //            recordNumber = current;
                    //        }
                    //    }
                    //
                    // 4. Create the result record:
                    //    ChunkResult result = new ChunkResult(chunk.chunkId(), chunk.start(), chunk.end(), longestChain, recordNumber, workerId);
                    //
                    // 5. Serialize the ChunkResult to JSON string and publish to RESULTS_TOPIC:
                    //    String jsonResult = mapper.writeValueAsString(result);
                    //    producer.send(new ProducerRecord<>(KafkaConfig.RESULTS_TOPIC, result.chunkId(), jsonResult));
                    //
                    // 6. Manually commit offsets to Kafka to acknowledge completion of this chunk:
                    //    consumer.commitSync();
                    //    System.out.printf("[%s] Successfully processed and committed chunk %s. Record number: %s with chain length: %d%n",
                    //            workerId, chunk.chunkId(), recordNumber, longestChain);
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } catch (Exception e) {
            System.err.println("Error in worker execution loop: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                consumer.close();
                producer.close();
                System.out.printf("[%s] Worker resources released successfully.%n", workerId);
            } catch (Exception e) {
                System.err.println("Error closing worker resources: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.run();
    }
}
