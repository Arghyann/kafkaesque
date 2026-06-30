package com.collatz;

import java.math.BigInteger;
import java.util.HashSet;

/**
 * Phase 1 — Core Collatz logic
 *
 * The Collatz conjecture states that for any positive integer n:
 * - If n is even, the next number is n / 2.
 * - If n is odd, the next number is 3 * n + 1.
 * - If we repeat this process, we eventually reach 1.
 *
 * Your goal: Implement the calculateChainLength method.
 *
 * Learning moments:
 * - Why use BigInteger instead of long? What is the maximum number a standard 64-bit signed long can hold,
 *   and why might Collatz sequences for large ranges exceed this?
 * - How to perform basic arithmetic (addition, multiplication, division by 2, comparison) using BigInteger.
 * - How to write clean, loop-based calculations without causing a StackOverflowError (which recursion could cause).
 */
public class CollatzVerifier {

    private static final BigInteger TWO = BigInteger.valueOf(2);
    private static final BigInteger THREE = BigInteger.valueOf(3);
    private static final BigInteger ONE = BigInteger.valueOf(1);
    private static final BigInteger ZERO = BigInteger.valueOf(0);
    private static int MAX_STEPS=100000;

    private static BigInteger next(BigInteger n) {
        if (!n.testBit(0)) {
            return n.shiftRight(1);
        } else {
            return n.multiply(THREE).add(ONE);
        }
    }

    public static long calculateChainLength(BigInteger start) {
        if (start == null || start.compareTo(ZERO) <= 0) {
            throw new IllegalArgumentException("Starting number must be greater than 0");
        }

        long steps = 0;
        BigInteger current = start;
        BigInteger slow = start;
        BigInteger fast = start;
        boolean cycleCheckActive = true;

        while (!current.equals(ONE)) {
            if (steps >= MAX_STEPS) {
                throw new IllegalStateException("Max steps reached - potential cycle or extremely long chain");
            }

            current = next(current);
            steps++;

            // Floyd's Cycle Detection
            if (cycleCheckActive) {
                BigInteger fastNext = next(fast);
                if (fastNext.equals(ONE)) {
                    cycleCheckActive = false;
                } else {
                    fast = next(fastNext);
                    slow = next(slow);

                    if (fast.equals(ONE)) {
                        cycleCheckActive = false;
                    } else if (slow.equals(fast)) {
                        throw new IllegalStateException("Cycle detected");
                    }
                }
            }
        }

        return steps + 1; // Include the starting number in length
    }
}
