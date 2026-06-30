package com.collatz;

import java.math.BigInteger;

/**
 * Represents the results of processing a ChunkRange.
 */
public record ChunkResult(
        String chunkId,
        BigInteger start,
        BigInteger end,
        long longestChain,
        BigInteger recordNumber,
        String workerId
) {}
