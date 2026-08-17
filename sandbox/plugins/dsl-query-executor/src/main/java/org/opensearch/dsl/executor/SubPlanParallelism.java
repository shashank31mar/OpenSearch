/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import java.util.OptionalInt;

/**
 * How many sub-plans of one DSL query may run concurrently ("K_eff"), as a pure function over an
 * injected {@link Inputs} record. Nothing here reads a setting, a thread pool or a cluster state —
 * every input has a named producer at the call site, which is what makes the whole grid unit-testable
 * without a live node.
 *
 * <h2>The formula</h2>
 * <pre>
 * F        = max(1, min(shardsOnBusiestNode, maxConcurrentShardRequestsPerNode))   // fragments per sub-query
 * A        = max(1, floor(vCpu * fragmentExecutorMultiplier / targetPartitions))   // FRAGMENTS the gate admits
 * K_gate   = max(1, ceil(A / F))                                                  // SUB-QUERIES that fits
 * K_search = max(1, floor((searchPoolSize - SEARCH_RESERVE) / F))
 * K_eff    = clamp(min(gatedPlans, kSetting, [K_gate], [K_search]), 1, gatedPlans)
 * </pre>
 * {@code gatedPlans} is how many of the query's {@code n} plans actually go through the permit gate, and
 * it is the <b>only</b> term the launch mode changes: the staged launch holds plan 0 back to warm the
 * metadata cache, so {@code gatedPlans = n - 1} (which is what {@link #decide(Inputs)} assumes and what
 * every shipped path uses); the experimental flat launch sends all of them, so {@code gatedPlans = n}.
 * {@code K_gate} counts <b>sub-queries</b>, not fragments: every other term in the {@code min} counts
 * sub-queries and one sub-query costs {@code F} fragments, hence the division. Do not "simplify"
 * {@code ceil(A / F)} back to {@code A} and do not turn the {@code ceil} into a {@code floor} — a
 * {@code floor} pins {@code K_eff = 1} on an 8-vCPU / 2-shard node, the cell the gain model predicts
 * 1.50x for. {@code SubPlanParallelismTests#testKEffGridMatchesGainModel} exists to make both
 * un-revertible.
 *
 * <h2>Two terms are droppable, and they drop the same way</h2>
 * {@code K_gate} is absent when no installed backend declares the concurrency-gate multiplier (the
 * Lucene backend has no gate at all); {@code K_search} is absent when the SEARCH executor is not an
 * {@code OpenSearchThreadPoolExecutor}, so its size cannot be read. In both cases the term <b>leaves
 * the {@code min}</b> — it is never substituted with a guessed value, because a synthesised value is
 * indistinguishable from a real one and would clamp where the contract says drop. {@code gatedPlans} and
 * {@code kSetting} are always present, so the {@code min} can never be empty.
 *
 * <h2>K_eff is advisory, not authoritative</h2>
 * It is computed on the <b>coordinator</b> for a concurrency gate and a SEARCH pool that live on
 * <b>data nodes</b>; on a heterogeneous cluster, or against a dedicated coordinator, it describes the
 * wrong machine. Two different vCPU notions also sit inside one {@code min}: {@code A} and
 * {@code targetPartitions} derive from raw {@code Runtime.getRuntime().availableProcessors()}, while
 * SEARCH pool sizing derives from {@code allocatedProcessors} / {@code node.processors}. Treat the
 * result as a bound that is usually right, never as an exact capacity statement.
 */
public final class SubPlanParallelism {

    /**
     * SEARCH threads held back from the fan-out. An <b>invented constant</b> (design open question),
     * not a value derived from any setting or from server code: the coordinator itself occupies one
     * SEARCH thread for the duration of the fan-out (the fan-out loop runs on it), so the reserve is
     * that thread plus one unit of headroom.
     */
    static final int SEARCH_RESERVE = 2;

    /**
     * Above this many fan-out plans the caller takes the sequential path instead. Bounds the inline
     * drain depth: {@code PendingExecutions.finishAndRunNext} drains the next queued task on the
     * <i>finishing</i> thread, so a callee that completes inline nests one
     * {@code finishAndRunNext -> tryRun -> run -> execute} frame group per plan.
     */
    static final int MAX_FANOUT_PLANS = 8;

    /**
     * Hard ceiling on the operator-set K, re-applied here rather than trusted from the setting.
     * {@code dsl.query.max_parallel_sub_plans} enforces the same maximum in its own
     * {@code Setting}, but this class is handed a plain {@code int}: a caller that ever reads the value
     * from somewhere else must not be able to widen the fan-out past what was measured.
     */
    static final int MAX_K_SETTING = 2;

    private SubPlanParallelism() {}

    /**
     * Every input the {@code K_eff} decision needs, injected so the formula is testable without a live
     * node. Each field's producer is named in its own comment; this class reads nothing itself.
     *
     * @param n number of plans in the query ({@code QueryPlans.getAll().size()})
     * @param kSetting the operator's {@code dsl.query.max_parallel_sub_plans}, re-clamped here to
     *                 {@code [1, MAX_K_SETTING]}
     * @param vCpu {@code Runtime.getRuntime().availableProcessors()} on the coordinator
     * @param fragmentExecutorMultiplier the backend's concurrency-gate multiplier, unwrapped;
     *                                   <b>unread</b> when {@code gateTermPresent} is false, so the
     *                                   caller passes a deliberately poisonous placeholder there
     * @param targetPartitions the backend's derived {@code target_partitions}, already {@code >= 1}
     * @param gateTermPresent whether a backend declared the multiplier at all; {@code false} DROPS the
     *                        {@code K_gate} term rather than clamping it to 1
     * @param shardsOnBusiestNode S_node, from live coordinator routing
     * @param maxConcurrentShardRequestsPerNode the engine's per-node in-flight shard-request cap
     * @param searchPoolSize live {@code getMaximumPoolSize()} of the SEARCH executor;
     *                       {@link OptionalInt#empty()} DROPS the {@code K_search} term
     */
    public record Inputs(int n, int kSetting, int vCpu, double fragmentExecutorMultiplier, int targetPartitions, boolean gateTermPresent,
        int shardsOnBusiestNode, int maxConcurrentShardRequestsPerNode, OptionalInt searchPoolSize) {
    }

    /**
     * The chosen width together with every intermediate term that produced it, so the one observable
     * {@code K_eff} log line can report the values actually used instead of recomputing them (a second
     * computation could disagree with the first and the line would then describe a run that never
     * happened).
     *
     * @param kEff the effective number of sub-plans that may run concurrently, always {@code >= 1}
     * @param a fragments the concurrency gate admits per node; not meaningful when {@code kGate} is
     *          empty, because there is then no multiplier to derive it from
     * @param f fragments one sub-query costs on the busiest node
     * @param kGate the gate-derived sub-query bound, or empty when that term was dropped
     * @param kSearch the SEARCH-pool-derived sub-query bound, or empty when that term was dropped
     */
    public record Decision(int kEff, int a, int f, OptionalInt kGate, OptionalInt kSearch) {
    }

    /**
     * The effective fan-out width for the given inputs.
     *
     * <p><b>Package-private, and it stays that way.</b> Nothing in production calls this — the decision site
     * needs the intermediate terms for the width line, so it calls {@link #decide(Inputs)}. This is the
     * grid tests' shorthand for the one number, kept only because those tests read better for it. Public
     * it would be a second entry point to the formula for an outside caller to grow a second definition
     * behind, which is exactly the drift {@code SubPlanParallelismTests} exists to prevent.
     *
     * @param in the injected inputs
     * @return {@code K_eff}, always in {@code [1, max(1, n - 1)]}
     */
    static int computeKEff(Inputs in) {
        return decide(in).kEff();
    }

    /**
     * The same computation as {@link #computeKEff(Inputs)}, keeping the intermediate terms for the
     * observability line, for the <b>staged</b> launch — the shipped shape, where plan 0 runs alone and
     * only {@code n - 1} plans reach the gate.
     *
     * @param in the injected inputs
     * @return the width plus the terms it was derived from
     */
    public static Decision decide(Inputs in) {
        return decide(in, in.n() - 1);
    }

    /**
     * The width for a launch that sends {@code gatedPlans} of the query's plans through the permit gate.
     *
     * <p>Package-private, and the reason mirrors {@link #computeKEff(Inputs)}: the only caller is
     * {@code DslQueryPlanExecutor}, and a second public entry point to the formula is a place for a second
     * definition to grow. It exists because the flat launch of design experiment E5 gates {@code n} plans
     * rather than {@code n - 1} — <b>the only difference between the two launch shapes' widths</b>. Note
     * that {@code in.n()} is still the query's real plan count (it is what the width line reports); it is
     * deliberately not overwritten with a synthetic value to fake a wider clamp.
     *
     * @param in the injected inputs
     * @param gatedPlans how many plans this launch sends through the gate: {@code n - 1} staged, {@code n} flat
     * @return the width plus the terms it was derived from, with {@code kEff} in {@code [1, max(1, gatedPlans)]}
     */
    static Decision decide(Inputs in, int gatedPlans) {
        // Both belts, deliberately: F feeds two divisions, and a shard count of 0 (red index) or a
        // relaxed cap on the declaring plugin's side must not reach them.
        int f = Math.max(1, Math.min(in.shardsOnBusiestNode(), in.maxConcurrentShardRequestsPerNode()));

        int a;
        OptionalInt kGate;
        if (in.gateTermPresent()) {
            // The producer clamps targetPartitions to >= 1; clamped again here because this record is a
            // seam that another reader could fill differently, and a 0 here would throw on the query
            // path — the one thing an advisory input must never do.
            int targetPartitions = Math.max(1, in.targetPartitions());
            a = Math.max(1, (int) Math.floor(in.vCpu() * in.fragmentExecutorMultiplier() / targetPartitions));
            kGate = OptionalInt.of(Math.max(1, ceilDiv(a, f)));
        } else {
            // No multiplier was declared, so there are no gate-admitted fragments to count. The term is
            // dropped below; `a` is reported as 1 only so the log line has a number, and the accompanying
            // `K_gate=absent` is what tells a reader it took no part in the decision. The multiplier
            // itself is never read on this branch — that is why the caller may pass NaN for it.
            a = 1;
            kGate = OptionalInt.empty();
        }

        OptionalInt kSearch = in.searchPoolSize().isPresent()
            ? OptionalInt.of(Math.max(1, (in.searchPoolSize().getAsInt() - SEARCH_RESERVE) / f))
            : OptionalInt.empty();

        // A launch with nothing (or one thing) to gate has no width decision to make, and
        // clamp(..., 1, gatedPlans) would be malformed at 0 (upper < lower). Returning 1 also keeps the
        // caller from ever constructing a PendingExecutions(0), whose constructor asserts permits > 0.
        // At gatedPlans == 1 the full computation below produces 1 as well, so this is a short circuit
        // rather than a second answer.
        if (gatedPlans <= 1) {
            return new Decision(1, a, f, kGate, kSearch);
        }

        int kEff = Math.min(gatedPlans, Math.max(1, Math.min(MAX_K_SETTING, in.kSetting())));
        if (kGate.isPresent()) {
            kEff = Math.min(kEff, kGate.getAsInt());
        }
        if (kSearch.isPresent()) {
            kEff = Math.min(kEff, kSearch.getAsInt());
        }
        // Both ends of the clamp: every term above is >= 1, so this only re-states the invariant the
        // callers rely on (a gate of width 0 or a width above the gated plan count).
        kEff = Math.max(1, Math.min(kEff, gatedPlans));
        return new Decision(kEff, a, f, kGate, kSearch);
    }

    /** Integer ceiling division for two positive ints. */
    private static int ceilDiv(int dividend, int divisor) {
        return (dividend + divisor - 1) / divisor;
    }
}
