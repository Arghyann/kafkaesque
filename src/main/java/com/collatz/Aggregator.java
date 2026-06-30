package com.collatz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Phase 4 — Aggregator Node
 *
 * The Aggregator consumes results from the "results" topic and prints running statistics:
 * - Total numbers verified
 * - Global longest chain length and the number that produced it
 * - Throughput (numbers verified per second)
 *
 * Learning moments:
 * - Independent Consumer Group: Why does the Aggregator need a different group.id than the Workers?
 *   (Answer: If they shared a group ID, they would compete for partitions and the aggregator wouldn't receive all results!)
 * - State tracking: Storing and updating state locally inside a streaming system.
 */
public class Aggregator {

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Running stats state
    private BigInteger totalVerified = BigInteger.ZERO;
    private long globalLongestChain = 0;
    private BigInteger globalRecordNumber = BigInteger.ZERO;
    private long startTimeMillis = 0;

    public Aggregator() {
        this.mapper = new ObjectMapper();

        // Initialize Consumer properties with a separate group ID so it receives all results
        Properties consumerProps = KafkaConfig.getConsumerProperties("collatz-aggregator-group");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(KafkaConfig.RESULTS_TOPIC));
        System.out.printf("Aggregator started. Subscribed to topic: %s%n", KafkaConfig.RESULTS_TOPIC);

        startTimeMillis = System.currentTimeMillis();

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down aggregator...");
            closed.set(true);
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    ChunkResult result = mapper.readValue(record.value(), ChunkResult.class);

                    BigInteger size = result.end().subtract(result.start()).add(BigInteger.ONE);

                    totalVerified = totalVerified.add(size);

                    if (result.longestChain() > globalLongestChain) {
                        globalLongestChain = result.longestChain();
                        globalRecordNumber = result.recordNumber();
                    }

                    consumer.commitSync();

                    long elapsedSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000;
                    elapsedSeconds = elapsedSeconds == 0 ? 1 : elapsedSeconds;

                    BigInteger throughput =
                            totalVerified.divide(BigInteger.valueOf(elapsedSeconds));

                    System.out.println("\n=== Collatz Verification Dashboard ===");
                    System.out.println("Status: ACTIVE");
                    System.out.println("Total numbers verified: " + totalVerified);
                    System.out.println("Current throughput:     " + throughput + " numbers/sec");
                    System.out.println("Longest chain found:    " + globalLongestChain +
                            " (produced by number: " + globalRecordNumber + ")");
                    System.out.println("Last processed chunk:   " + result.chunkId() +
                            " (by worker: " + result.workerId() + ")");
                    System.out.println("======================================");
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } catch (Exception e) {
            System.err.println("Error in aggregator execution loop: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                consumer.close();
                System.out.println("Aggregator resources released successfully.");
            } catch (Exception e) {
                System.err.println("Error closing aggregator: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        Aggregator aggregator = new Aggregator();
        aggregator.run();
    }
}
