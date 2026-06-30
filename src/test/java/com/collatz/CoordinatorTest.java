package com.collatz;

import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorTest {

    @Test
    public void testEvenSplit() {
        // [1, 9] into 3 chunks: [1, 3], [4, 6], [7, 9]
        List<Coordinator.ChunkRange> chunks = Coordinator.splitIntoChunks(
                BigInteger.ONE, BigInteger.valueOf(9), 3
        );

        assertEquals(3, chunks.size());
        assertEquals(BigInteger.valueOf(1), chunks.get(0).start());
        assertEquals(BigInteger.valueOf(3), chunks.get(0).end());

        assertEquals(BigInteger.valueOf(4), chunks.get(1).start());
        assertEquals(BigInteger.valueOf(6), chunks.get(1).end());

        assertEquals(BigInteger.valueOf(7), chunks.get(2).start());
        assertEquals(BigInteger.valueOf(9), chunks.get(2).end());
    }

    @Test
    public void testUnevenSplit() {
        // [1, 10] into 3 chunks:
        // Size is 10. 10 / 3 = 3 remainder 1.
        // Chunk 0 gets size 4 (1 to 4)
        // Chunk 1 gets size 3 (5 to 7)
        // Chunk 2 gets size 3 (8 to 10)
        List<Coordinator.ChunkRange> chunks = Coordinator.splitIntoChunks(
                BigInteger.ONE, BigInteger.valueOf(10), 3
        );

        assertEquals(3, chunks.size());
        assertEquals(BigInteger.valueOf(1), chunks.get(0).start());
        assertEquals(BigInteger.valueOf(4), chunks.get(0).end());

        assertEquals(BigInteger.valueOf(5), chunks.get(1).start());
        assertEquals(BigInteger.valueOf(7), chunks.get(1).end());

        assertEquals(BigInteger.valueOf(8), chunks.get(2).start());
        assertEquals(BigInteger.valueOf(10), chunks.get(2).end());
    }

    @Test
    public void testContinuity() {
        BigInteger start = BigInteger.valueOf(100);
        BigInteger end = BigInteger.valueOf(1053);
        int numChunks = 17;

        List<Coordinator.ChunkRange> chunks = Coordinator.splitIntoChunks(start, end, numChunks);
        assertEquals(numChunks, chunks.size());

        // First chunk start and last chunk end must match bounds
        assertEquals(start, chunks.get(0).start());
        assertEquals(end, chunks.get(numChunks - 1).end());

        // Ensure there are no gaps or overlaps
        for (int i = 0; i < numChunks - 1; i++) {
            BigInteger currentEnd = chunks.get(i).end();
            BigInteger nextStart = chunks.get(i + 1).start();
            assertEquals(currentEnd.add(BigInteger.ONE), nextStart, 
                    "Gap or overlap detected between chunk " + i + " and " + (i + 1));
        }
    }
}
