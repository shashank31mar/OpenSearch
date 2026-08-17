/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.result.ExecutionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Gathers the results of one query's concurrently executing sub-plans into <b>plan order</b> and fires
 * the request's listener exactly once, when the last of them has reported.
 *
 * <p>Modelled on the engine's per-shard stitcher — an {@code AtomicInteger} countdown plus a queue of
 * failures, one terminal, no short-circuit — but deliberately local code rather than a dependency on it:
 * this collects sub-plan results on the coordinator, not Arrow batches on a stage.
 *
 * <p>Two shapes are load-bearing:
 * <ul>
 *   <li><b>Slot by plan index, not by completion order.</b> The emitted order has to be deterministic,
 *       and the concurrent slots replace the plain {@code ArrayList} the sequential chain used to append
 *       to. ({@code GroupedActionListener} slots by completion order, so it cannot be used here.)</li>
 *   <li><b>No short-circuit on the first failure.</b> Firing the listener early would leave up to
 *       {@code K_eff - 1} distributed queries running with nothing left to report to. Every sub-plan is
 *       waited for; the first failure is the one reported and the rest are logged (see {@link #finish}).</li>
 * </ul>
 *
 * <p>The reported exception is <b>not</b> guaranteed to be customer-safe: the engine's exception
 * converter has no front-end caller yet, so a backend error can arrive here unconverted. Wiring that up
 * is out of scope — do not assume it happened. What this class does control is the <em>number</em> of
 * internal exceptions one failed {@code _search} can leak: exactly one, never {@code K_eff}. That is why
 * the sibling failures are logged rather than attached with {@code addSuppressed} — REST error rendering
 * walks the suppressed chain.
 */
final class SubPlanResultCollector {

    private static final Logger logger = LogManager.getLogger(SubPlanResultCollector.class);

    private final int n;
    private final int reportingPlans;
    private final AtomicArray<ExecutionResult> slots;
    private final AtomicInteger pending;
    private final ConcurrentLinkedQueue<Exception> failures = new ConcurrentLinkedQueue<>();
    private final ActionListener<List<ExecutionResult>> outer;

    /**
     * A collector for the <b>staged</b> launch: plan 0 has already completed, so only the {@code n - 1}
     * fanned-out plans report through {@link #planSucceeded} / {@link #planFailed} and plan 0's result
     * arrives through {@link #slotZero}.
     *
     * @param n total number of plans in the query, including plan 0; must be at least 2
     * @param outer the request's listener, fired exactly once
     * @throws IllegalArgumentException if {@code n < 2}, which would make the countdown unable to complete
     */
    SubPlanResultCollector(int n, ActionListener<List<ExecutionResult>> outer) {
        this(n, n - 1, outer);
    }

    /**
     * @param n total number of plans in the query, including plan 0; must be at least 2
     * @param reportingPlans how many of those plans will report through {@link #planSucceeded} /
     *                       {@link #planFailed}, i.e. how many go through the permit gate: {@code n - 1}
     *                       for the staged launch (plan 0 ran before this collector existed and is slotted
     *                       by {@link #slotZero}), {@code n} for the flat launch of experiment E5, where
     *                       plan 0 is gated like any other and {@code slotZero} is not used at all
     * @param outer the request's listener, fired exactly once
     * @throws IllegalArgumentException if {@code n < 2}, or if {@code reportingPlans} is not in
     *                                  {@code [1, n]} — both would make the countdown unable to complete
     */
    SubPlanResultCollector(int n, int reportingPlans, ActionListener<List<ExecutionResult>> outer) {
        if (n < 2) {
            // Rejected rather than tolerated, because the tolerated form is a HANG. With n <= 1 the
            // countdown below starts at or below 0, nothing ever calls countDown() (slotZero deliberately
            // does not) and finish() therefore never runs, so the request's listener is never fired and its
            // REST channel is held open forever — the one failure mode this class exists to prevent. Two
            // callers already keep this from happening (the single-plan early return and the sequential
            // branch taken at width 1), so this makes the invariant the collector's own instead of theirs.
            // Throwing is also the fail-secure direction: the caller constructs the collector inside plan
            // 0's completion callback, whose wrapper routes a throw to the request's failure arm, so a
            // construction bug surfaces as an honest error response rather than as a hung request.
            throw new IllegalArgumentException("a fan-out collector needs at least 2 plans, got " + n);
        }
        if (reportingPlans < 1 || reportingPlans > n) {
            // The same hang, one argument further out: a countdown that starts at 0 (or below) never
            // reaches its terminal, and one that starts above n can never be driven to zero because there
            // are only n plans to report. Rejected here for the same reason as n < 2 — loudly, on the
            // caller's thread, rather than as a REST channel held open forever.
            throw new IllegalArgumentException(
                "a fan-out collector of " + n + " plans needs 1.." + n + " reporters, got " + reportingPlans
            );
        }
        this.n = n;
        this.reportingPlans = reportingPlans;
        this.slots = new AtomicArray<>(n);
        // How many plans still owe a report. Staged passes n - 1: plan 0 has already completed by the time
        // that collector exists — it is dispatched alone to warm the node's metadata cache — and its result
        // arrives through slotZero, which deliberately does not count down. The flat launch gates plan 0
        // too, so it passes n and every plan counts itself down.
        this.pending = new AtomicInteger(reportingPlans);
        this.outer = outer;
    }

    /**
     * Rejects a dispatch that could not drive this countdown to its terminal — checked on the caller's
     * thread, before the first plan goes out, so nothing is in flight and no permit is held.
     *
     * <p>The dispatch range and this collector's report count are chosen <em>independently</em> by the two
     * launch arms ({@code from = 1} with {@code n - 1} reporters staged, {@code 0} with {@code n} flat), and
     * the countdown cannot notice a disagreement itself. Too MANY reporters is the failure this class exists
     * to prevent: {@code pending} never reaches 0, {@link #finish} never runs, the request's listener is
     * never fired and its REST channel is held open forever. Too FEW is worse than {@link #finish}'s
     * null-slot guard suggests: a staged {@code from} of 0 dispatches plan 0 a second time <em>and</em>
     * {@link #slotZero} still fills slot 0, so every slot is non-null and the terminal fires early with a
     * complete-looking result while a duplicate distributed query is still running — a fast-but-wrong
     * number on the E5 path.
     *
     * <p>Reported through {@code outer} rather than thrown. The flat arm — the only one that can reach a
     * mismatch — calls this on the request thread inside a plain runnable that the transport action does not
     * wrap, so a bare throw would leave the channel open: the very hang being guarded. Unconditional rather
     * than an {@code assert}, because assertions are on in the test JVMs and off in a distribution, i.e. off
     * precisely on the benchmark node.
     *
     * @param from the first plan index the caller is about to dispatch
     * @param n the caller's plan count, i.e. the exclusive upper bound of its dispatch
     * @return {@code false} when the pairing cannot terminate, having already failed the request's listener
     */
    boolean expectGatedRange(int from, int n) {
        if (n == this.n && reportingPlans == n - from) {
            return true;
        }
        IllegalStateException e = new IllegalStateException(
            "a fan-out collector of "
                + this.n
                + " plans expecting "
                + reportingPlans
                + " reports cannot be driven by a dispatch of plans ["
                + from
                + ", "
                + n
                + ")"
        );
        logger.error("dsl.fanout dispatch/report-count mismatch; the query is failed rather than hung", e);
        outer.onFailure(e);
        return false;
    }

    /**
     * Records plan 0's result without counting down: it completed before the fan-out started. Staged
     * launches only — under a flat launch plan 0 goes through the gate and reports through
     * {@link #planSucceeded} like every other plan, and calling this as well would leave the countdown one
     * report short of its terminal.
     *
     * @param result plan 0's result
     */
    void slotZero(ExecutionResult result) {
        slots.set(0, result);
    }

    /**
     * Records a fanned-out plan's result and counts down.
     *
     * @param index the plan's index in {@code QueryPlans.getAll()}
     * @param result that plan's result
     */
    void planSucceeded(int index, ExecutionResult result) {
        slots.set(index, result);
        countDown();
    }

    /**
     * Records a fanned-out plan's failure and counts down. Called for a plan that failed asynchronously
     * <i>and</i> for one that could not be dispatched at all — a plan that never decremented would leave
     * the listener uncompleted, i.e. a hung REST channel.
     *
     * @param e the failure
     */
    void planFailed(Exception e) {
        failures.offer(e);
        countDown();
    }

    /**
     * The single terminal. Both the success and the failure path go through here, rather than each
     * testing the countdown itself, so "fires exactly once" is a property of one line of code.
     */
    private void countDown() {
        if (pending.decrementAndGet() == 0) {
            finish();
        }
    }

    private void finish() {
        // Drained once, then never read again: poll() hands over the primary and the loop reports the rest,
        // so no failure is lost and none is reported twice.
        Exception primary = failures.poll();
        if (primary != null) {
            // The siblings are LOGGED, deliberately not attached to the reported exception with
            // addSuppressed(). OpenSearch's REST error rendering walks the suppressed chain, so attaching
            // them would emit one internal exception type and message per failed sub-plan to the client:
            // the sequential path leaked one, and a K-wide fan-out would multiply that by K. The customer
            // gets one failure for one _search; the operator gets all of them here, with their stack
            // traces. WARN because nothing else reports these at all — the primary is the only one the
            // request itself carries.
            int siblings = 0;
            for (Exception other = failures.poll(); other != null; other = failures.poll()) {
                final int sibling = ++siblings;
                logger.warn(
                    () -> new ParameterizedMessage(
                        "a further sub-plan of this {}-plan query failed (additional failure {}); it is "
                            + "reported here only, never attached to the failure returned to the client, "
                            + "which is [{}]",
                        n,
                        sibling,
                        primary
                    ),
                    other
                );
            }
            outer.onFailure(primary);
            return;
        }
        // Read by index into a fresh list rather than AtomicArray.asList(): that method SKIPS null slots
        // and memoizes the result, so an incompletely filled array would hand back a SHORT list that looks
        // complete — and the wrong value would then be cached forever.
        List<ExecutionResult> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ExecutionResult result = slots.get(i);
            if (result == null) {
                // Unreachable while every plan reports exactly once; kept because the alternative is
                // delivering a list with a null in it to a caller that indexes into it positionally.
                outer.onFailure(new IllegalStateException("sub-plan " + i + " of " + n + " reported neither a result nor a failure"));
                return;
            }
            results.add(result);
        }
        outer.onResponse(results);
    }
}
