package com.collatz;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Phase 2 — Chunking logic
 *
 * The Coordinator is responsible for splitting a massive number range [startRange, endRange]
 * into N chunks.
 *
 * Your goal: Implement the splitIntoChunks method and run the test/main method to verify.
 *
 * Learning moments:
 * - How to divide range size using BigInteger.
 * - Handling remainders/uneven distributions (if range size isn't perfectly divisible by N).
 * - Writing clean boundary checks.
 */
public class Coordinator {

    /**
     * Represents a single work assignment chunk.
     */
    public record ChunkRange(String chunkId, BigInteger start, BigInteger end) {
        @Override
        public String toString() {
            return String.format("Chunk[%s]: %s -> %s", chunkId, start, end);
        }
    }

    /**
     * Splits a range from startRange to endRange (inclusive) into N chunks.
     * For example, splitting [1, 10] into 3 chunks:
     * - Chunk 1: [1, 4]
     * - Chunk 2: [5, 7]
     * - Chunk 3: [8, 10]
     *
     * @param startRange Starting number of the full range (inclusive)
     * @param endRange Ending number of the full range (inclusive)
     * @param numChunks Number of chunks to split into
     * @return A list of ChunkRange objects.
     * @throws IllegalArgumentException if range is invalid or numChunks <= 0.
     */
    public static List<ChunkRange> splitIntoChunks(BigInteger startRange, BigInteger endRange, int numChunks) {
        if (startRange == null || endRange == null || startRange.compareTo(BigInteger.ZERO) <= 0) {
            throw new IllegalArgumentException("Ranges must be greater than 0");
        }
        if (startRange.compareTo(endRange) > 0) {
            throw new IllegalArgumentException("startRange must be <= endRange");
        }
        if (numChunks <= 0) {
            throw new IllegalArgumentException("numChunks must be greater than 0");
        }
        BigInteger totalSize = endRange.subtract(startRange).add(BigInteger.ONE);
        BigInteger numChunksBig = BigInteger.valueOf(numChunks);
        BigInteger sizePerChunk = totalSize.divide(numChunksBig);
        BigInteger remainder = totalSize.remainder(numChunksBig);

        List<ChunkRange> chunks = new ArrayList<>();
        BigInteger currentStart = startRange;

        for (int i = 0; i < numChunks; i++) {
            BigInteger currentSize = sizePerChunk;
            if (BigInteger.valueOf(i).compareTo(remainder) < 0) {
                currentSize = currentSize.add(BigInteger.ONE);
            }

            if (currentSize.compareTo(BigInteger.ZERO) <= 0) {
                break; // If range is smaller than numChunks, stop creating empty chunks
            }

            BigInteger currentEnd = currentStart.add(currentSize).subtract(BigInteger.ONE);
            chunks.add(new ChunkRange(
                    "chunk-" + (i + 1),
                    currentStart,
                    currentEnd
            ));
            currentStart = currentEnd.add(BigInteger.ONE);
        }

        return chunks;
    }

    /**
     * Publishes a list of ChunkRanges to the Kafka "work" topic.
     *
     * Learning moments:
     * - Creating a Producer: How to instantiate KafkaProducer with configuration properties.
     * - Serializing to JSON: Using Jackson ObjectMapper to convert objects to JSON Strings.
     * - Sending Records: Constructing ProducerRecord and calling producer.send().
     * - Flushing: Forcing Kafka's internal buffer to write messages to the broker immediately.
     */
    public static void publishChunksToKafka(List<ChunkRange> chunks) {
        System.out.println("Preparing to publish " + chunks.size() + " chunks to Kafka...");

        // TODO: Implement Kafka Producer wiring.
        // 1. Instantiate the Jackson ObjectMapper:
        //    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        //
        // 2. Create the KafkaProducer. Since keys and values are Strings, use:
        //    org.apache.kafka.clients.producer.KafkaProducer<String, String> producer =
        //         new org.apache.kafka.clients.producer.KafkaProducer<>(KafkaConfig.getProducerProperties());
        //
        // 3. Try-with-resources (or try-finally) to make sure the producer is closed safely:
        //    try (producer) {
        //        for (ChunkRange chunk : chunks) {
        //            // Convert chunk object to JSON string:
        //            String jsonValue = mapper.writeValueAsString(chunk);
        //
        //            // Create a ProducerRecord (topic, key, value)
        //            org.apache.kafka.clients.producer.ProducerRecord<String, String> record =
        //                 new org.apache.kafka.clients.producer.ProducerRecord<>(
        //                      KafkaConfig.WORK_TOPIC, chunk.chunkId(), jsonValue
        //                 );
        //
        //            // Send the record asynchronously. You can optionally add a Callback to print success/failure details:
        //            producer.send(record, (metadata, exception) -> {
        //                if (exception != null) {
        //                    System.err.println("Error publishing chunk: " + exception.getMessage());
        //                } else {
        //                    System.out.printf("Sent chunk %s to partition %d at offset %d%n",
        //                            chunk.chunkId(), metadata.partition(), metadata.offset());
        //                }
        //            });
        //        }
        //        // Flush to ensure all buffered messages are sent before exiting
        //        producer.flush();
        //    } catch (Exception e) {
        //        System.err.println("Failed to publish chunks: " + e.getMessage());
        //    }

        System.out.println("Publishing finished.");
    }

    public static void main(String[] args) {
        // Define a range to test: e.g., 1 to 100,000, split into 10 chunks
        BigInteger start = BigInteger.ONE;
        BigInteger end = BigInteger.valueOf(100_000);
        int chunksCount = 10;

        List<ChunkRange> chunks = splitIntoChunks(start, end, chunksCount);
        for (ChunkRange chunk : chunks) {
            System.out.println(chunk);
        }

        // TODO: Uncomment this after implementing publishChunksToKafka:
        // publishChunksToKafka(chunks);
    }
}
