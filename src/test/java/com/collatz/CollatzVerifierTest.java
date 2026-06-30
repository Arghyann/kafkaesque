package com.collatz;

import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import static org.junit.jupiter.api.Assertions.*;

public class CollatzVerifierTest {

    @Test
    public void testChainForOne() {
        // 1 -> length should be 1
        assertEquals(1, CollatzVerifier.calculateChainLength(BigInteger.ONE));
    }

    @Test
    public void testChainForSix() {
        // 6 -> 3 -> 10 -> 5 -> 16 -> 8 -> 4 -> 2 -> 1 (9 elements)
        assertEquals(9, CollatzVerifier.calculateChainLength(BigInteger.valueOf(6)));
    }

    @Test
    public void testChainForFive() {
        // 5 -> 16 -> 8 -> 4 -> 2 -> 1 (6 elements)
        assertEquals(6, CollatzVerifier.calculateChainLength(BigInteger.valueOf(5)));
    }

    @Test
    public void testInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> {
            CollatzVerifier.calculateChainLength(BigInteger.ZERO);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            CollatzVerifier.calculateChainLength(BigInteger.valueOf(-5));
        });

        assertThrows(IllegalArgumentException.class, () -> {
            CollatzVerifier.calculateChainLength(null);
        });
    }
}
