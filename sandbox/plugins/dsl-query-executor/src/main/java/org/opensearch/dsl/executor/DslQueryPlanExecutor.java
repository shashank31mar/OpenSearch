/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.settings.DslGateInputs;
import org.opensearch.dsl.settings.DslQuerySettings;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;

/**
 * Executes the plans of one DSL query through the analytics engine's {@link QueryPlanExecutor} and
 * collects their results in plan order.
 *
 * <p>Execution is <b>staged</b> by default: plan 0 runs alone, and only once it has succeeded do the
 * remaining {@code n - 1} plans go out through a permit gate of width {@code K_eff}
 * ({@link SubPlanParallelism}). Plan 0 first is not cosmetic — a plan whose parquet metadata is not yet
 * cached on the data node runs <i>unbudgeted</i> there, so launching all {@code K} at once would run
 * them all unbudgeted at exactly the moment {@code K} times the memory is in flight. The warm is
 * node-local and each plan resolves its own shard copies, so staging lowers the <em>expected</em> number
 * of concurrent unbudgeted queries rather than eliminating it.
 *
 * <p><b>The launch shape is switchable, for measurement only</b>
 * ({@code dsl.query.fanout_launch}, default {@code staged} — see
 * {@link DslQuerySettings#FANOUT_LAUNCH}). Staging costs the fan-out its first wave: the width can never
 * exceed {@code n - 1}, so a 2-plan query — a 2-level nested aggregation with {@code size: 0}, the
 * measured production shape — runs strictly sequentially however wide the gate is set. The design's
 * experiment <b>E5 ("cold/warm x FLAT vs STAGED launch")</b> exists to decide whether that trade is
 * right and has never been run, because until now there was nothing to compare against. Under
 * {@code flat} all {@code n} plans go through the gate from the start, so {@code K_eff} can reach
 * {@code n} — and no plan warms the metadata cache first, which is precisely the risk staging was
 * chosen to reduce. Everything else is identical in both shapes: results are slotted by plan index,
 * the request's listener fires exactly once, the first failure is the one reported, and no failure
 * short-circuits a sibling that is still running.
 *
 * <p>At {@code K_eff == 1} — the shipped default, and also what the guards below fall back to — the call
 * sequence is the sequential chain this class has always used: plan {@code i + 1} is dispatched only
 * after plan {@code i} succeeds, and the first failure aborts the chain. The fan-out path deliberately
 * differs on that second point: it waits for every sub-plan before reporting a failure, because
 * short-circuiting would abandon sibling queries that are still running.
 *
 * <p><b>Nothing above this class observes result order today</b> — the response builder discards the
 * results entirely — so the ordering contract lives in this class's unit tests and nowhere else.
 */
public class DslQueryPlanExecutor {

    private static final Logger logger = LogManager.getLogger(DslQueryPlanExecutor.class);

    /**
     * The three non-numeric renderings of a width-line term. Part of SC-10's contract and NOT
     * interchangeable — {@link #logKEff} documents what each one tells a reader. Kept as constants so the
     * strings the runbook matches on exist in exactly one place.
     */
    private static final String TERM_ABSENT = "absent";
    private static final String TERM_SKIPPED = "skipped";
    private static final String TERM_UNAVAILABLE = "unavailable";

    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final DslQuerySettings dslSettings;
    private final DslGateInputs gateInputs;

    /**
     * Creates an executor backed by the given analytics engine plan executor.
     *
     * @param executor analytics engine executor that runs individual RelNode plans
     * @param clusterService supplies the coordinator's operation routing, for the shard-layout input of
     *                       the fan-out width
     * @param threadPool supplies the live SEARCH executor, for the pool-size input of the fan-out width
     * @param dslSettings holder of {@code dsl.query.max_parallel_sub_plans}, read once per query
     * @param gateInputs reader for the cross-plugin concurrency-gate inputs, read once per query
     */
    public DslQueryPlanExecutor(
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor,
        ClusterService clusterService,
        ThreadPool threadPool,
        DslQuerySettings dslSettings,
        DslGateInputs gateInputs
    ) {
        this.executor = executor;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.dslSettings = dslSettings;
        this.gateInputs = gateInputs;
    }

    // TODO: add per-plan error handling so a failure in one plan
    // doesn't prevent returning partial results from other plans (e.g. HITS)
    /**
     * Executes all plans of one request and delivers their results, in plan order, to the listener.
     *
     * <p>Under the default staged launch, plan 0 is dispatched alone. When it fails, the listener fails
     * with that error and no fan-out is started. When it succeeds, {@code K_eff} is computed once (and
     * logged once — the only observable the rollout and the benchmark cells attribute against) and the
     * remaining plans run either sequentially ({@code K_eff == 1}) or through a permit gate. Under the
     * experimental flat launch the width is settled <em>before</em> anything is dispatched and all
     * {@code n} plans go through the gate — see {@link #launchFlat}.
     *
     * <p>The launch mode is read <b>once per query</b>, here: it is a dynamic setting, and an update that
     * landed between plan 0 and the fan-out would otherwise split one query across two launch shapes.
     * A single-plan query takes the same path under either mode (there is nothing to fan out, and it
     * emits no width line).
     *
     * <p>A synchronous throw out of plan 0's dispatch propagates to the caller. A synchronous throw out of
     * a <em>fanned-out</em> plan's dispatch is caught here instead: it happens on a finishing thread with a
     * permit held, so it has to release the permit and drive the countdown rather than escape.
     *
     * <p>{@code state} is the request's single cluster-state snapshot and is read for <b>one</b> purpose:
     * the shard-layout term of the fan-out width. It is deliberately <i>not</i> threaded into
     * {@code QueryPlanExecutor#execute}, which still receives {@code null} exactly as the sequential path
     * always has — handing the engine a per-request context (and with it a parent task for cancellation
     * propagation) is a separate change to the request path, not part of the fan-out.
     *
     * @param plans the query plans to execute
     * @param state the request's cluster-state snapshot, read only for the shard-layout width input; a
     *              {@code null} drops that input
     * @param concreteIndex the request's single resolved concrete index, used only to read the shard
     *                      layout the fan-out width divides by; a {@code null} drops that input
     * @param listener receives the ordered list of results on success, or the failure
     */
    public void execute(QueryPlans plans, ClusterState state, String concreteIndex, ActionListener<List<ExecutionResult>> listener) {
        List<QueryPlans.QueryPlan> queryPlans = plans.getAll();
        final int n = queryPlans.size();
        if (n == 0) {
            listener.onResponse(List.of());
            return;
        }
        if (n > 1 && dslSettings.fanoutLaunch() == DslQuerySettings.LaunchMode.FLAT) {
            launchFlat(queryPlans, n, state, concreteIndex, listener);
            return;
        }
        final QueryPlans.QueryPlan plan0 = queryPlans.get(0);
        logPlan(plan0.relNode());
        // TODO: context param is null, may carry execution hints
        executor.execute(plan0.relNode(), null, ActionListener.wrap(rows -> {
            logRows(rows);
            ExecutionResult result0 = new ExecutionResult(plan0, rows);
            if (n == 1) {
                // A single-plan query never reaches the fan-out decision, so it emits no K_eff line. An
                // absent line means "not a multi-plan query", never "the line was dropped".
                List<ExecutionResult> results = new ArrayList<>(1);
                results.add(result0);
                listener.onResponse(results);
                return;
            }
            fanOut(queryPlans, n, result0, state, concreteIndex, listener);
        }, listener::onFailure));
    }

    /**
     * The <b>flat</b> launch of experiment E5: settle the width first, then send all {@code n} plans
     * through one permit gate. No plan runs alone, so {@code K_eff} is clamped to {@code n} rather than to
     * {@code n - 1} and a 2-plan query can finally reach a width of 2 — the entire reason this arm exists.
     *
     * <p>Deliberately <em>not</em> a warm-up-free copy of {@link #fanOut}: it reuses that method's gate
     * loop verbatim ({@link #dispatchGated}) and its collector, so the ordering, once-only-completion and
     * permit accounting of the two shapes cannot drift apart. The only differences are the first gated
     * index (0 rather than 1) and the collector's expected report count ({@code n} rather than
     * {@code n - 1}), because here plan 0 is a gated plan like any other and {@code slotZero} is unused.
     *
     * <p>Two consequences of dispatching plan 0 through the gate are worth naming, because they are real
     * behaviour differences and not oversights:
     * <ul>
     *   <li>The width is decided on the <b>calling</b> thread, before any plan runs, so a query whose
     *       plan 0 fails no longer skips the width read. {@link #decideWidth} still cannot throw, so
     *       nothing here can fail a search that the staged path would have answered.</li>
     *   <li>A plan-0 failure no longer suppresses the other plans: they are already dispatched, so the
     *       collector waits for all of them and reports plan 0's failure once, exactly as it would for any
     *       other plan. Short-circuiting instead would abandon in-flight distributed queries.</li>
     * </ul>
     */
    private void launchFlat(
        List<QueryPlans.QueryPlan> queryPlans,
        int n,
        ClusterState state,
        String concreteIndex,
        ActionListener<List<ExecutionResult>> listener
    ) {
        final int kEff = decideWidth(n, n, DslQuerySettings.LaunchMode.FLAT, state, concreteIndex);
        if (kEff == 1) {
            // A width-1 gate is the sequential chain with extra bookkeeping, so take the chain itself —
            // the same one the staged path takes at width 1, from plan 0 instead of plan 1. That keeps the
            // degraded flat arm byte-identical to the sequential baseline it is measured against.
            executeNext(queryPlans, 0, new ArrayList<>(n), listener);
            return;
        }
        dispatchGated(queryPlans, 0, n, new SubPlanResultCollector(n, n, listener), new PendingExecutions(kEff));
    }

    /**
     * The {@code K_eff} decision: settles the width (emitting the one observability line for this query on
     * every path through here) and never throws. Reached from plan 0's callback under a staged launch, and
     * from the calling thread before any dispatch under a flat one.
     *
     * <p><b>Not throwing is the contract, not defensiveness.</b> Every individual input is already
     * fail-secure on its own — {@code DslGateInputs} catches per key and drops the term,
     * {@code CoordinatorShardLayout} degrades to 1 — but under a staged launch this <em>composition</em>
     * runs inside plan 0's success callback, whose {@code ActionListener.wrap} routes anything thrown here
     * to the request's failure arm. A width read that threw would therefore fail a search whose plan 0 had
     * already succeeded, which contradicts the one tenet this whole path is built on: a wrong fan-out width
     * must never fail a search. (Under a flat launch it runs before any dispatch, where a throw would
     * instead escape {@code execute} into the caller — the same tenet, one frame further out.) Two reads
     * here are unguarded by their own producers and can genuinely throw —
     * {@code DslGateInputs.targetPartitions()} resolves two {@code :server} settings <em>typed</em>
     * ({@code ClusterSettings.get(Setting)} throws when a key stops being registered, e.g. after an
     * upgrade), and {@code threadPool.executor(SEARCH)} throws if that pool is not registered. Both
     * degrade to the sequential path.
     *
     * <p>The width is settled in the {@code try} and logged after it, so the line is emitted exactly once
     * on all three paths and no path can emit it twice.
     *
     * @param n number of plans in this query, always {@code >= 2} here
     * @param gatedPlans how many of them this launch sends through the gate: {@code n - 1} staged, {@code n} flat
     * @param launch the launch mode this query is running under, reported on the width line so a benchmark
     *               cell can attribute a number to an arm
     * @param state the request's snapshot, source of the shard-layout read
     * @param concreteIndex the request's resolved concrete index, or null
     * @return the width to run at, always {@code >= 1}
     */
    private int decideWidth(int n, int gatedPlans, DslQuerySettings.LaunchMode launch, ClusterState state, String concreteIndex) {
        // Boxed so "not read yet" is representable: the sentinel renders as the state string below. Today
        // this read cannot throw (DslQuerySettings caches the value in a volatile field and refreshes it
        // from a settings-update consumer), which is exactly why it is the read taken FIRST — but a later
        // change to a live read must not be able to skip the line.
        Integer kSetting = null;
        SubPlanParallelism.Decision decision = null;
        String unread = TERM_SKIPPED;
        try {
            kSetting = dslSettings.maxParallelSubPlans();
            // Bound the inline drain depth: PendingExecutions.finishAndRunNext drains the next queued task
            // on the finishing thread, so a callee that completes inline nests one frame group per plan.
            // Above the bound, take the sequential path instead of trampolining every hand-off through the
            // SEARCH pool — that would add a SEARCH admission per plan completion on a path whose
            // coordinator demand is already K_eff + 1. Counted in GATED plans, not in n: the nesting comes
            // from the plans that go through the gate, which is n - 1 staged and n flat. At K_setting = 2
            // that shift gives E5 two plan counts where the arms run at different widths, and they are NOT
            // the same kind of thing.
            // A VALID CELL at n == 2: staged gates 1, which short-circuits to K_eff = 1 (SubPlanParallelism's
            // gatedPlans <= 1 branch), while flat gates 2 and reaches 2. That difference IS the launch shape
            // under measurement — the reason the flat arm exists at all.
            // NOT A COMPARABLE CELL at n == MAX_FANOUT_PLANS + 1: staged gates 8 and stays under this bound,
            // flat gates 9 and falls back to sequential, so the width gap there measures THIS bound and not
            // the launch shape. E5's grid is the n = 3 ladder, so no planned cell lands on it; keep it so.
            // Same width is therefore not the comparability test either way: before comparing two arms, read
            // the K_eff each one actually ran at off the width line, which carries launch= for exactly that.
            boolean tooManyPlans = gatedPlans > SubPlanParallelism.MAX_FANOUT_PLANS;
            // Ordered cheapest-decisive-first, deliberately. At the shipped default
            // (max_parallel_sub_plans = 1) and above the plan bound, K_eff is 1 whatever every other term
            // says, so reading them would be pure waste on the query hot path: readInputs() runs
            // OperationRouting.searchShards over every shard of the index (a fresh GroupShardsIterator plus
            // a per-node HashMap) and four settings lookups, all of it then discarded. Their fields render
            // as `skipped` below rather than as a number, because "never read" and "read and dropped"
            // (`absent`) mean opposite things to the runbook.
            if (kSetting > 1 && tooManyPlans == false) {
                decision = SubPlanParallelism.decide(readInputs(n, kSetting, state, concreteIndex), gatedPlans);
            }
        } catch (RuntimeException e) {
            // DEBUG, not WARN: on a node whose registry really did lose a key this fires on every
            // multi-plan query, and the search itself is unaffected — it just runs sequentially, which is
            // the shipped default anyway.
            logger.debug("the fan-out width could not be read; running the sub-plans sequentially", e);
            decision = null;
            unread = TERM_UNAVAILABLE;
        }
        int kEff = decision == null ? 1 : decision.kEff();
        logKEff(kSetting, decision, unread, n, kEff, launch);
        return kEff;
    }

    /**
     * Dispatches the remaining plans at the settled width: either the sequential chain or a permit gate.
     * The staged launch's half of the decision — plan 0 has already succeeded when this runs.
     */
    private void fanOut(
        List<QueryPlans.QueryPlan> queryPlans,
        int n,
        ExecutionResult result0,
        ClusterState state,
        String concreteIndex,
        ActionListener<List<ExecutionResult>> listener
    ) {
        final int kEff = decideWidth(n, n - 1, DslQuerySettings.LaunchMode.STAGED, state, concreteIndex);

        if (kEff == 1) {
            // Byte-identical to the pre-fan-out behaviour, including its fail-fast: the sequential path is
            // what the shipped default (max_parallel_sub_plans = 1) takes.
            List<ExecutionResult> results = new ArrayList<>(n);
            results.add(result0);
            executeNext(queryPlans, 1, results, listener);
            return;
        }

        SubPlanResultCollector collector = new SubPlanResultCollector(n, listener);
        collector.slotZero(result0);
        dispatchGated(queryPlans, 1, n, collector, new PendingExecutions(kEff));
    }

    /**
     * Sends plans {@code [from, n)} through one permit gate, reporting each to the collector by its own
     * plan index. The single copy of the fan-out's permit accounting: the staged launch enters at
     * {@code from = 1} (plan 0 already ran alone and was slotted), the flat launch at {@code from = 0}.
     *
     * <p>A synchronous throw out of a plan's dispatch is caught here rather than propagated: it happens on
     * a finishing thread with a permit held, so it has to release the permit and drive the countdown rather
     * than escape.
     *
     * @param queryPlans the query's plans
     * @param from the first plan index to gate
     * @param n the query's plan count, i.e. the exclusive upper bound of the dispatch
     * @param collector the collector every gated plan reports to; must be sized for exactly {@code n - from}
     *                  reports, which is checked before the first dispatch — a mismatch fails the request
     *                  through the collector rather than hanging it, and nothing is dispatched
     * @param gate a fresh gate of the settled width, owned by this dispatch alone
     */
    private void dispatchGated(
        List<QueryPlans.QueryPlan> queryPlans,
        int from,
        int n,
        SubPlanResultCollector collector,
        PendingExecutions gate
    ) {
        // The dispatch range and the collector's report count are chosen by different launch arms; a
        // disagreement is a HANG one way and an early terminal with a duplicate query still in flight the
        // other, never a merely wrong value. Checked before the loop, so bailing out leaks no permit and
        // abandons no in-flight plan. STAGED (from = 1, n - 1 reporters) can never take this branch.
        if (collector.expectGatedRange(from, n) == false) {
            return;
        }
        for (int i = from; i < n; i++) {
            final int idx = i;
            final QueryPlans.QueryPlan plan = queryPlans.get(idx);
            // notifyOnce is OUTERMOST, and that order is load-bearing. runAfter fires its Runnable from a
            // finally on EVERY notification and has no once-only guard of its own, so with notifyOnce on the
            // inside a listener that is notified twice (see the catch below — the engine can complete the
            // listener and *then* throw) would release the permit twice: finishAndRunNext would decrement
            // past its own count and drain an extra queued task, admitting one plan more than K_eff.
            // notifyOnce on the outside makes the second notification a no-op before runAfter can see it.
            ActionListener<Iterable<Object[]>> perPlan = ActionListener.notifyOnce(
                ActionListener.runAfter(reportTo(collector, idx, plan), gate::finishAndRunNext)
            );
            // PendingExecutions.tryRun takes a plain Runnable on this branch, so every admitted plan
            // unconditionally owes exactly one finishAndRunNext — which perPlan pays on every outcome,
            // including the catch below. There is no "declined" path to represent: the only caller that
            // ever needed one is request cancellation, which is not part of the fan-out.
            gate.tryRun(() -> {
                try {
                    // Same thread, strictly before this plan's dispatch: that program order is the
                    // happens-before edge that keeps this plan's invalidateMetadataQuery() from racing the
                    // engine's own invalidate for the same plan. Never in a completion callback, never
                    // outside tryRun. At the shipped INFO level the whole call costs one isDebugEnabled()
                    // boolean. Inside the try, not above it: at DEBUG it dereferences the plan's metadata
                    // provider and renders the plan, and a throw from there escaping this runnable would
                    // leak the permit it already took and skip the countdown, so the listener would never
                    // fire and the REST channel would hang. On the drain path the throw would also escape
                    // into the engine's completion thread through finishAndRunNext.
                    logPlan(plan.relNode());
                    // TODO: context param is null, may carry execution hints
                    executor.execute(plan.relNode(), null, perPlan);
                } catch (Exception e) {
                    // A plan that cannot be dispatched must still release its permit and count down, or the
                    // listener never fires and the REST channel hangs. perPlan does both in one call.
                    perPlan.onFailure(e);
                }
            });
        }
    }

    /**
     * One fanned-out plan's outcome, reported to the collector exactly once.
     *
     * <p>Written out rather than assembled from {@code ActionListener.wrap}, and the difference is not
     * stylistic. {@code wrap} runs its success body inside a {@code try} and routes anything the body
     * throws to its own failure arm — so with the countdown call in that body, a throw from the
     * <em>terminal</em> (the last plan's countdown reaches zero, and the request's own listener throws out
     * of {@code onResponse}) would come back as a second report for the same plan: the countdown would run
     * twice, drop below zero, and the exception would sit unread in the collector's failure queue while
     * nothing was left to fire. Here the row handling that legitimately can fail is inside the {@code try}
     * and the report is outside it, so each plan reports exactly once and a terminal's throw propagates to
     * its caller instead of being laundered into this plan's failure.
     *
     * @param collector the query's result collector
     * @param idx this plan's index in the query's plan list
     * @param plan the plan being dispatched, carried into its result
     * @return the listener to hand to the engine for that plan
     */
    private ActionListener<Iterable<Object[]>> reportTo(SubPlanResultCollector collector, int idx, QueryPlans.QueryPlan plan) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Iterable<Object[]> rows) {
                ExecutionResult result;
                try {
                    logRows(rows);
                    result = new ExecutionResult(plan, rows);
                } catch (Exception e) {
                    // A failure while handling this plan's rows is this plan's failure, and it has to count
                    // down like any other or the request never completes.
                    collector.planFailed(e);
                    return;
                }
                collector.planSucceeded(idx, result);
            }

            @Override
            public void onFailure(Exception e) {
                collector.planFailed(e);
            }
        };
    }

    /**
     * The sequential chain: dispatch plan {@code index}, and only from its success callback dispatch
     * plan {@code index + 1}. The first failure ends the chain — the listener fires {@code onFailure} with
     * that error and the remaining plans do not run.
     *
     * <p>Entered at {@code index = 1} by the staged launch (plan 0 having already run and been appended to
     * {@code results}) and at {@code index = 0} by a flat launch that settled on width 1, where there is
     * nothing for a gate to overlap and the chain is the baseline both arms are measured against.
     */
    private void executeNext(
        List<QueryPlans.QueryPlan> queryPlans,
        int index,
        List<ExecutionResult> results,
        ActionListener<List<ExecutionResult>> outer
    ) {
        if (index >= queryPlans.size()) {
            outer.onResponse(results);
            return;
        }
        QueryPlans.QueryPlan plan = queryPlans.get(index);
        RelNode relNode = plan.relNode();
        logPlan(relNode);
        // Sequential dispatch: a synchronous throw from here propagates out through the caller's
        // completion callback, where ActionListener.wrap routes it to the listener's failure arm.
        // TODO: context param is null, may carry execution hints
        executor.execute(relNode, null, ActionListener.wrap(rows -> {
            logRows(rows);
            results.add(new ExecutionResult(plan, rows));
            executeNext(queryPlans, index + 1, results, outer);
        }, outer::onFailure));
    }

    /**
     * Builds the fan-out width's inputs once per query, at the decision site, from their named producers.
     * Read here and not cached: the gate inputs are all dynamic settings, and an operator sweeping them
     * has to be able to change the width of a running node.
     *
     * <p>Reached <b>only</b> when the width is not already decided by {@code kSetting} or the plan bound —
     * see {@link #decideWidth}. Every read below costs something on the query hot path, and the caller
     * passes {@code kSetting} in rather than letting this method re-read it, so the value that gated the
     * call is the value that is reported.
     */
    private SubPlanParallelism.Inputs readInputs(int n, int kSetting, ClusterState state, String concreteIndex) {
        OptionalDouble multiplier = gateInputs.fragmentExecutorMultiplier();
        return new SubPlanParallelism.Inputs(
            n,
            kSetting,
            Runtime.getRuntime().availableProcessors(),
            // Deliberately NaN and not 1.0: when the multiplier is absent the gate term is DROPPED, so this
            // value is never read — and a synthesised 1.0 would be indistinguishable from a genuinely
            // configured 1.0, i.e. it would clamp the width where the contract says drop the term.
            multiplier.orElse(Double.NaN),
            gateInputs.targetPartitions(),
            multiplier.isPresent(),
            shardsOnBusiestNode(state, concreteIndex),
            gateInputs.maxConcurrentShardRequestsPerNode(),
            searchPoolSize(threadPool.executor(ThreadPool.Names.SEARCH))
        );
    }

    /**
     * Shards of this request's index on the busiest node, read from the <em>request's own</em>
     * cluster-state snapshot. Not a second {@code clusterService.state()} read: two reads of one request
     * can straddle a routing change and produce a layout that never existed on any state.
     */
    private int shardsOnBusiestNode(ClusterState state, String concreteIndex) {
        if (state == null || concreteIndex == null) {
            // No snapshot to read routing from. 1 is the neutral value for a count, and F = max(1, ...)
            // guards the division either way. Note the DIRECTION: a shard count of 1 makes F = 1, which
            // makes both derived terms as LARGE as they can be, i.e. it widens rather than narrows. That is
            // why the caller threads the request's real snapshot through instead of relying on this branch.
            return 1;
        }
        return CoordinatorShardLayout.shardsOnBusiestNode(state, clusterService.operationRouting(), concreteIndex);
    }

    /**
     * Live maximum size of the SEARCH pool, or empty when it cannot be read.
     *
     * <p>{@code getMaximumPoolSize()} on the executor itself, deliberately: {@code ThreadPool.info} is
     * built once at node start and never rebuilt, so it goes stale the moment the pool is resized through
     * {@code cluster.thread_pool.*}, and {@code _nodes/stats}' thread count is the lazily-grown
     * <i>current</i> size, which on an idle node is below the configured one.
     *
     * <p>An executor that is not an {@code OpenSearchThreadPoolExecutor} yields empty, which <b>drops</b>
     * the SEARCH term from the fan-out width. There is deliberately no fallback pool-size constant
     * anywhere on this path: the only candidate formula is the <i>default</i> size, which is wrong on any
     * node that configured or resized the pool, and guessing it would silently widen or narrow the fan-out
     * against a pool that does not exist. {@code instanceof} rather than a blind cast for the same reason
     * — fail secure, never a {@code ClassCastException} on the query path.
     *
     * @param searchExecutor the SEARCH executor, as returned by {@code ThreadPool.executor}
     * @return its live maximum pool size, or empty
     */
    static OptionalInt searchPoolSize(ExecutorService searchExecutor) {
        return (searchExecutor instanceof OpenSearchThreadPoolExecutor tpe)
            ? OptionalInt.of(tpe.getMaximumPoolSize())
            : OptionalInt.empty();
    }

    /**
     * The one line per multi-plan query that makes the fan-out width observable — the rollout steps and
     * every benchmark cell attribute against it, and without it a run whose width was pinned to 1 by the
     * gate is indistinguishable from a K=1 baseline.
     *
     * <p>Contract, not style: the fixed leading token {@code dsl.fanout.k_eff} (what the runbook greps
     * for), then the original seven fields in this order, then {@code launch}, INFO level (the runbook
     * reads it on a production canary, where DEBUG is off), once per query at the decision site — not per
     * plan, not in the loop.
     *
     * <p>{@code launch} is appended rather than inserted, and it is not optional. It names the launch shape
     * ({@code staged} / {@code flat}) that produced this width, and without it the two arms of experiment
     * E5 are indistinguishable in the logs: the same {@code K_setting} and {@code n} yield different widths
     * per arm, so a scraped {@code K_eff} could not be attributed to the arm that produced it. Appending
     * keeps the leading token and the seven original field positions intact for anything already matching
     * the line; a reader that anchored its regex to the end of the line has to be updated once.
     *
     * <p><b>TEMPORARY at INFO, for the rollout only.</b> One line per multi-plan search is unbounded volume
     * on the query hot path, and the only way to silence it today is to raise this whole class's logger,
     * which also silences everything else it reports. It is INFO because the rollout steps and WS-E's
     * benchmark attribution read it off a production canary where DEBUG is off — <b>do not demote or sample
     * it while that attribution is live.</b> Once the rollout completes, sample it (per-node counter, or one
     * line per width <em>change</em>) or demote it to DEBUG.
     *
     * <p>A term can render three non-numeric ways, and they mean three different things — a reader that
     * collapses them is wrong:
     * <ul>
     *   <li>{@code absent} — the term was read and <b>dropped from the {@code min}</b> (no gated backend
     *       declared the multiplier; the SEARCH pool size is unreadable). {@code K_gate=1} (clamped) and
     *       {@code K_gate=absent} (dropped) mean opposite things.</li>
     *   <li>{@code skipped} — the term was <b>never read</b>, because {@code K_setting <= 1} or the plan
     *       count is above the inline-drain bound already settled the width at 1. Not a degradation: it is
     *       what the shipped default does, and the reads it avoids are the expensive ones.</li>
     *   <li>{@code unavailable} — reading the inputs <b>threw</b>, so the width degraded to 1. The cause is
     *       at DEBUG on this logger.</li>
     * </ul>
     * A missing line still means "not a multi-plan query" and nothing else, which is why the three states
     * above exist rather than an early return.
     *
     * <p>Nothing plan-shaped may join this line: it is emitted on a completion thread, which has not set
     * Calcite's metadata-provider ThreadLocal, so stringifying a {@code RelNode} here would NPE. That is
     * why it does not reuse {@code logPlan}, whose safety comes from setting that ThreadLocal itself.
     *
     * @param kSetting the operator's width setting, or null if even that could not be read
     * @param decision the terms actually computed, or null when they were skipped or unavailable
     * @param unread how to render the terms {@code decision} does not carry
     * @param n the query's plan count
     * @param kEff the width this query runs at
     * @param launch the launch shape that produced that width
     */
    private void logKEff(
        Integer kSetting,
        SubPlanParallelism.Decision decision,
        String unread,
        int n,
        int kEff,
        DslQuerySettings.LaunchMode launch
    ) {
        logger.info(
            "dsl.fanout.k_eff K_setting={} A={} F={} K_gate={} K_search={} n={} K_eff={} launch={}",
            kSetting == null ? unread : String.valueOf(kSetting),
            decision == null ? unread : String.valueOf(decision.a()),
            decision == null ? unread : String.valueOf(decision.f()),
            decision == null ? unread : bound(decision.kGate()),
            decision == null ? unread : bound(decision.kSearch()),
            n,
            kEff,
            launch.settingValue()
        );
    }

    /** A droppable term of the width line: its value, or {@link #TERM_ABSENT} when it left the min. */
    private static String bound(OptionalInt term) {
        return term.isPresent() ? String.valueOf(term.getAsInt()) : TERM_ABSENT;
    }

    private static void logRows(Iterable<Object[]> rows) {
        if (logger.isDebugEnabled() == false) return;
        List<Object[]> list = (rows instanceof List) ? (List<Object[]>) rows : null;
        int count = list != null ? list.size() : -1;
        logger.debug("Query result rowCount={}", count);
        if (list != null) {
            int preview = Math.min(20, list.size());
            for (int i = 0; i < preview; i++) {
                logger.debug("row[{}]={}", i, Arrays.toString(list.get(i)));
            }
            if (list.size() > preview) {
                logger.debug("... ({} more rows)", list.size() - preview);
            }
        }
    }

    /**
     * Logs a plan's text at DEBUG, and is the reference for how to do that safely.
     *
     * <p>All three statements are load-bearing and none is removable. {@code RelWriterImpl} calls
     * {@code getCluster().getMetadataQuery()} unconditionally while rendering, and a metadata query built
     * on a thread whose {@code THREAD_PROVIDERS} ThreadLocal is unset NPEs — so the set comes first, and
     * <b>the invalidate is exactly what makes the set load-bearing</b>, because it forces the next
     * {@code getMetadataQuery()} to build a fresh metadata query on <em>this</em> thread. (The invalidate
     * touches only this plan's own {@code RelOptCluster}: plans are converted one cluster each.)
     *
     * <p>Callers must keep the invariant: this runs on the same thread as, and strictly before, the
     * dispatch of the <em>same</em> plan. Moving it into a completion callback reintroduces the torn
     * metadata-query NPE. The {@code isDebugEnabled} guard decides only whether the block runs, it does
     * not make an out-of-order call safe — a DEBUG-only NPE is still a production NPE, hit by whoever
     * turned DEBUG on to debug something else.
     *
     * <p>DEBUG rather than INFO is a volume decision, not a correctness one: at the shipped default level
     * this fired a full plan text on every request, and would now fire {@code K} of them per query. The
     * level is a dynamic node setting, so both this and {@code logRows} stay reachable without a rebuild.
     *
     * <p><b>The ThreadLocal is restored in a {@code finally}, and that is a leak fix, not tidiness.</b> This
     * method is reached from plan 0's completion callback, i.e. on a pooled ENGINE/TRANSPORT thread rather
     * than on the coordinator's own thread. A bare {@code set} leaves the {@code JaninoRelMetadataProvider}
     * — and transitively this query's {@code RelOptCluster}, its {@code SchemaPlus} and the
     * {@code ClusterState} they reach — pinned to that shared thread after the request ends, where an
     * unrelated later query can observe it. The previous value is restored rather than blindly removed
     * because the calling thread may legitimately be mid-plan with its own provider set.
     */
    private void logPlan(RelNode relNode) {
        if (logger.isDebugEnabled()) {
            org.apache.calcite.rel.metadata.JaninoRelMetadataProvider previous =
                org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.get();
            try {
                org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.set(
                    org.apache.calcite.rel.metadata.JaninoRelMetadataProvider.of(
                        java.util.Objects.requireNonNull(relNode.getCluster().getMetadataProvider())
                    )
                );
                relNode.getCluster().invalidateMetadataQuery();
                logger.debug("Executing RelNode:\n{}", relNode.explain());
            } finally {
                if (previous == null) {
                    org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                } else {
                    org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.set(previous);
                }
            }
        }
    }
}
