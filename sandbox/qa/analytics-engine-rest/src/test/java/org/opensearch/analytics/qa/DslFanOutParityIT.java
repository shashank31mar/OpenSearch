/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.junit.After;

import java.io.IOException;
import java.util.Map;

/**
 * The fan-out produces the <b>right</b> aggregation, not merely the right amount of work — at
 * {@code max_parallel_sub_plans = 2}.
 *
 * <p><b>The gap this closes, and nothing else does.</b> {@code DslFanOutIT} states its own ceiling: a
 * result slotted into the wrong index, or a lost, reordered or duplicated result object, is invisible from
 * there, so its across-width test compares {@code fragments.total} and
 * {@code fragment_executor_gate.total_batches_started} <em>deltas</em> — work done, not output — and it
 * forbids response-body assertions outright. {@link DslLegacyParityIT} compares output but runs at the
 * shipped default width of 1, so it says nothing about the fan-out. Concurrent completion order is exactly
 * the condition under which a positional join mis-slots, and this is the only place output is compared
 * under it. Without this class the fan-out has no correctness coverage.
 *
 * <p>Own {@code integTestDslFanoutParity} task and cluster, and excluded from the default
 * {@code integTest}: it flips two cluster-wide settings, and a leaked {@code max_parallel_sub_plans = 2}
 * would turn every later IT in that single-fork suite into an untracked fan-out test. Both are cleared in
 * {@link #restoreSettings()} — {@code dsl.query.enabled} back to its <b>default</b> ({@code false}), not to
 * {@code true}.
 *
 * <p>{@code max_parallel_sub_plans} is capped at 2 in the setting itself, so there is deliberately no
 * {@code = 3} case: that PUT is a {@code 400} by design, and it is the settings IT's assertion, not this
 * one's.
 */
public class DslFanOutParityIT extends DslParityTestBase {

    /**
     * A nondeterministic mis-join does not reproduce on one request. This is the content-stability count;
     * it is distinct from {@code DslFanOutIT}'s repeat loop, which checks status and the absence of crash
     * markers rather than output.
     */
    private static final int REPEATS = 20;

    @After
    public void restoreSettings() throws IOException {
        putTransient(MAX_PARALLEL_SUB_PLANS_KEY, "null");
        putTransient(DSL_QUERY_ENABLED_KEY, "null");
    }

    /**
     * The reason this class exists: at width 2, the DSL path's nested aggregation must equal legacy's. This
     * is the first assertion in the whole feature that says the <em>fan-out</em> produced the
     * <em>correct</em> nested aggregation.
     */
    public void testFanOutAtKTwoMatchesLegacyAggregation() throws IOException {
        Map<String, Object> legacy = legacyAggregations(nestedAggBody());

        Map<String, Object> dsl = fanOutAggregations(nestedAggBody());

        assertEquals("the fan-out must return legacy's nested aggregation", legacy, dsl);
    }

    /**
     * Width changed, output did not. This is the output half of {@code DslFanOutIT}'s work-delta test, which
     * compares stats across the same two widths and deliberately asserts nothing about the body.
     *
     * <p>The {@code aggregations} section only: {@code took} and {@code _shards} differ legitimately between
     * two runs, so a whole-body comparison would be flaky for reasons unrelated to slotting.
     */
    public void testFanOutResponseEqualsSequentialResponse() throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "true");

        putTransient(MAX_PARALLEL_SUB_PLANS_KEY, "1");
        Map<String, Object> sequential = aggregationsOf(DSL_INDEX, nestedAggBody());

        putTransient(MAX_PARALLEL_SUB_PLANS_KEY, "2");
        Map<String, Object> fannedOut = aggregationsOf(DSL_INDEX, nestedAggBody());

        assertEquals("fan-out must not change the aggregation output", sequential, fannedOut);
    }

    /**
     * The same-field sibling shape at width 2. The granularity key and the map-keyed join are what keep two
     * aggregations over one field apart, and out-of-order completion is precisely when a positional join
     * would swap them — a swap that width 1 cannot expose.
     */
    public void testFanOutSiblingTermsOnSameFieldMatchLegacy() throws IOException {
        Map<String, Object> legacy = legacyAggregations(siblingAggBody());
        assertEquals("the reference itself must carry both siblings: " + legacy, 2, legacy.size());

        Map<String, Object> dsl = fanOutAggregations(siblingAggBody());

        assertEquals("two aggregations over one field must survive the fan-out separately", legacy, dsl);
    }

    /**
     * Content stability across {@value #REPEATS} identical runs at width 2. A mis-join that depends on which
     * plan finishes first is intermittent by nature, so a single comparison can pass on a run that happened
     * to complete in plan order. Every iteration must produce the identical section.
     */
    public void testFanOutRepeatedRunsAreStable() throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "true");
        putTransient(MAX_PARALLEL_SUB_PLANS_KEY, "2");

        Map<String, Object> first = aggregationsOf(DSL_INDEX, nestedAggBody());
        for (int i = 1; i < REPEATS; i++) {
            assertEquals("iteration " + i + " produced a different aggregation section", first, aggregationsOf(DSL_INDEX, nestedAggBody()));
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /** The legacy reference: the Lucene twin with the DSL path explicitly off. */
    private Map<String, Object> legacyAggregations(String body) throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "false");
        return aggregationsOf(LEGACY_INDEX, body);
    }

    /** The DSL path at width 2 over the parquet twin. */
    private Map<String, Object> fanOutAggregations(String body) throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "true");
        putTransient(MAX_PARALLEL_SUB_PLANS_KEY, "2");
        return aggregationsOf(DSL_INDEX, body);
    }
}
