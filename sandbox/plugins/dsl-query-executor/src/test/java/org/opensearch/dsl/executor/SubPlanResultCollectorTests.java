/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The collector's three contracts: one terminal, plan order regardless of completion order, and no
 * short-circuit on a failure.
 */
public class SubPlanResultCollectorTests extends OpenSearchTestCase {

    /** Bound on the concurrent test's start barrier and joins, so a stuck thread fails rather than hangs. */
    private static final int TIMEOUT_SECONDS = 30;

    /**
     * A plan count the countdown could never work for is rejected at construction, from the collector's own
     * side. Two callers keep it from happening today — the single-plan early return and the sequential branch
     * taken at width 1 — but neither of them pins it here, and the tolerated form of this bug is the worst
     * one available: with the countdown starting at or below zero nothing would ever reach the terminal, the
     * request's listener would never be notified, and its REST channel would stay open forever. Failing loudly
     * is also the fail-secure direction, because the caller constructs the collector inside a completion
     * callback whose wrapper turns a throw into the request's failure.
     */
    public void testRejectsAPlanCountThatCouldNeverCompleteTheCountdown() {
        for (int n : new int[] { -1, 0, 1 }) {
            CapturingListener listener = new CapturingListener();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SubPlanResultCollector(n, listener));
            assertEquals("a fan-out collector needs at least 2 plans, got " + n, e.getMessage());
            assertEquals("the listener must not have been touched", 0, listener.terminalCalls);
        }
        // The boundary the other side of it: 2 is the smallest workable count and must construct.
        assertNotNull(new SubPlanResultCollector(2, new CapturingListener()));
    }

    public void testFiresOnceWhenAllSlotsFilled() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);

        ExecutionResult zero = result();
        ExecutionResult one = result();
        ExecutionResult two = result();
        collector.slotZero(zero);
        // Reverse completion order on purpose: the emitted order is by plan index, not by arrival.
        collector.planSucceeded(2, two);
        assertEquals("the listener must not fire before the last plan reports", 0, listener.terminalCalls);
        collector.planSucceeded(1, one);

        assertEquals(1, listener.terminalCalls);
        assertNull(listener.failure);
        assertEquals(List.of(zero, one, two), listener.results);
    }

    public void testDoesNotShortCircuitOnFailure() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        collector.slotZero(result());

        collector.planFailed(new IllegalStateException("plan 1 failed"));
        assertEquals("short-circuiting would abandon plan 2 while it is still running", 0, listener.terminalCalls);

        collector.planSucceeded(2, result());
        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.failure);
    }

    /**
     * One failed {@code _search}, <b>one</b> internal exception on the wire — never one per sub-plan.
     *
     * <p>This test previously asserted the opposite (the siblings {@code addSuppressed} onto the primary),
     * and the contract was changed deliberately: OpenSearch's REST error rendering walks the suppressed
     * chain, so a {@code K}-wide fan-out would have emitted {@code K} internal exception types and messages
     * to the client where the sequential path emitted one. The customer-facing count is what the first two
     * assertions pin.
     *
     * <p>The third is what keeps the fix from being a silent loss: the sibling has to be <em>reported
     * somewhere</em>, and the only place left is this node's log. Asserting the logged event carries the
     * sibling as its {@code thrown} — not merely that some line was emitted — is what makes "logged, not
     * dropped" mechanical.
     */
    public void testSiblingFailuresAreLoggedNotCarriedToTheClient() throws Exception {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        collector.slotZero(result());

        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second");
        List<Throwable> logged = new ArrayList<>();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SubPlanResultCollector.class))) {
            appender.addExpectation(new ThrownCapturingExpectation(logged));
            collector.planFailed(first);
            collector.planFailed(second);
        }

        assertSame("the first failure offered is the one the request reports", first, listener.failure);
        assertEquals(
            "a sibling attached with addSuppressed would be rendered to the client too: "
                + Arrays.toString(listener.failure.getSuppressed()),
            0,
            listener.failure.getSuppressed().length
        );
        assertEquals("the sibling must be reported server-side, or the fix loses it: " + logged, List.of(second), logged);
        assertEquals(1, listener.terminalCalls);
    }

    /** A plan that could not be dispatched at all still has to drive the countdown to zero. */
    public void testPreDispatchFailureStillDrivesCountdownToZero() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(2, listener);
        collector.slotZero(result());

        collector.planFailed(new IllegalStateException("never dispatched"));

        assertEquals("an undispatched plan must not leave the listener uncompleted", 1, listener.terminalCalls);
        assertNotNull(listener.failure);
    }

    /**
     * The {@code AtomicArray.asList()} hazard, stated as a test: on a partial failure nothing may escape
     * through {@code onResponse}, least of all a list shorter than the plan count that looks complete.
     */
    public void testNeverReturnsShortListOnPartialFailure() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        collector.slotZero(result());

        collector.planSucceeded(1, result());
        collector.planFailed(new IllegalStateException("plan 2 failed"));

        assertNull("a partial failure must not deliver results", listener.results);
        assertNotNull(listener.failure);
        assertEquals(1, listener.terminalCalls);
    }

    /**
     * The fail-secure guard on the success branch: a slot that was never filled (a wiring bug, not a
     * runtime condition) fails the request rather than handing a caller a list with a null in it, which
     * the caller reads positionally.
     */
    public void testMissingSlotFailsRatherThanDeliveringNulls() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        // slotZero deliberately not called.
        collector.planSucceeded(1, result());
        collector.planSucceeded(2, result());

        assertNull(listener.results);
        assertTrue(listener.failure instanceof IllegalStateException);
        assertEquals(1, listener.terminalCalls);
    }

    /** n = 9, eight reporters, one terminal. Run with {@code -Dtests.iters=100} for the race. */
    public void testConcurrentReportsFireListenerExactlyOnce() throws Exception {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(9, listener);
        collector.slotZero(result());

        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<ExecutionResult> expected = new ArrayList<>();
        expected.add(null); // slot 0 is filled above; replaced below for the assertion
        for (int i = 1; i < 9; i++) {
            final int idx = i;
            ExecutionResult result = result();
            expected.add(result);
            Thread thread = new Thread(() -> {
                try {
                    assertTrue(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                collector.planSucceeded(idx, result);
            }, "reporter-" + idx);
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("reporter thread did not finish", thread.isAlive());
        }

        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.results);
        assertEquals(9, listener.results.size());
        for (int i = 1; i < 9; i++) {
            assertSame("slot " + i + " must hold its own plan's result", expected.get(i), listener.results.get(i));
        }
    }

    /** Collects the {@code thrown} of every event the collector logs, i.e. the failures it reported itself. */
    private static class ThrownCapturingExpectation implements MockLogAppender.LoggingExpectation {

        private final List<Throwable> thrown;

        ThrownCapturingExpectation(List<Throwable> thrown) {
            this.thrown = thrown;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getThrown() != null) {
                thrown.add(event.getThrown());
            }
        }

        @Override
        public void assertMatched() {
            // The test asserts the contents; an expectation that failed on "no events" would say the same
            // thing twice and with a worse message.
        }
    }

    private static ExecutionResult result() {
        return new ExecutionResult(
            new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, TestUtils.createTestRelNode(), GranularityKeys.ROOT),
            List.<Object[]>of(new Object[] { COUNTER.incrementAndGet() })
        );
    }

    private static final AtomicInteger COUNTER = new AtomicInteger();

    /** Records the single terminal callback, so a double completion or a silent success is visible. */
    private static class CapturingListener implements ActionListener<List<ExecutionResult>> {

        private List<ExecutionResult> results;
        private Exception failure;
        private int terminalCalls;

        @Override
        public void onResponse(List<ExecutionResult> executionResults) {
            terminalCalls++;
            this.results = executionResults;
        }

        @Override
        public void onFailure(Exception e) {
            terminalCalls++;
            this.failure = e;
        }
    }
}
