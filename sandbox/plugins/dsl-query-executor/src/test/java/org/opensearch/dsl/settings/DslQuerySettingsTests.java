/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DslQuerySettingsTests extends OpenSearchTestCase {

    private static final String MAX_PARALLEL_KEY = "dsl.query.max_parallel_sub_plans";

    private static final String FANOUT_LAUNCH_KEY = "dsl.query.fanout_launch";

    // ── The setting descriptors ────────────────────────────────────────────

    public void testMaxParallelSubPlansDefaultIsOne() {
        assertEquals(MAX_PARALLEL_KEY, DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey());
        assertEquals(Integer.valueOf(1), DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.EMPTY));
    }

    public void testMaxParallelSubPlansRejectsZero() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.builder().put(MAX_PARALLEL_KEY, 0).build())
        );
        assertTrue("expected a lower-bound message, got: " + e.getMessage(), e.getMessage().contains("must be >= 1"));
    }

    /**
     * The cap has to be enforced by the {@code Setting} itself, not by a downstream {@code min} — this
     * is the assertion that fails if someone "relaxes" it.
     */
    public void testMaxParallelSubPlansRejectsThree() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.builder().put(MAX_PARALLEL_KEY, 3).build())
        );
        assertTrue("expected an upper-bound message, got: " + e.getMessage(), e.getMessage().contains("must be <= 2"));
    }

    public void testMaxParallelSubPlansAcceptsTwo() {
        assertEquals(Integer.valueOf(2), DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.builder().put(MAX_PARALLEL_KEY, 2).build()));
    }

    /**
     * Guards against a setting being added without {@code getSettings()} being updated (an unregistered key
     * is rejected in {@code opensearch.yml} and is a 400 through {@code _cluster/settings}), and against
     * {@code dsl.query.enabled} being re-introduced here: the interception gate belongs to
     * {@code SearchActionFilter.INTERCEPT_SEARCH_ENABLED}, and a duplicate lever in this list is the
     * drift this assertion exists to catch.
     *
     * <p>Deliberately widened from "only the width setting" to this exact pair when the experimental launch
     * mode was added — the guard is that the list matches the descriptors, not that it stays at one entry.
     */
    public void testAllContainsExactlyTheWidthAndLaunchSettings() {
        List<Setting<?>> all = DslQuerySettings.all();
        assertEquals("all() must contain exactly the width and launch settings, got " + keysOf(all), 2, all.size());
        assertEquals(Set.of(MAX_PARALLEL_KEY, FANOUT_LAUNCH_KEY), Set.copyOf(keysOf(all)));
    }

    // ── The experimental launch mode (design experiment E5) ────────────────

    /**
     * {@code staged} is the default, and that is the whole safety property of this setting: it is a
     * measurement switch, so an operator who never sets it must keep the shipped launch shape.
     */
    public void testFanoutLaunchDefaultsToStaged() {
        assertEquals(FANOUT_LAUNCH_KEY, DslQuerySettings.FANOUT_LAUNCH.getKey());
        assertEquals(DslQuerySettings.LaunchMode.STAGED, DslQuerySettings.FANOUT_LAUNCH.get(Settings.EMPTY));
    }

    public void testFanoutLaunchParsesBothArms() {
        assertEquals(
            DslQuerySettings.LaunchMode.STAGED,
            DslQuerySettings.FANOUT_LAUNCH.get(Settings.builder().put(FANOUT_LAUNCH_KEY, "staged").build())
        );
        assertEquals(
            DslQuerySettings.LaunchMode.FLAT,
            DslQuerySettings.FANOUT_LAUNCH.get(Settings.builder().put(FANOUT_LAUNCH_KEY, "flat").build())
        );
        // Case-insensitive on the way in; the width line always renders the canonical lower-case token.
        assertEquals(
            DslQuerySettings.LaunchMode.FLAT,
            DslQuerySettings.FANOUT_LAUNCH.get(Settings.builder().put(FANOUT_LAUNCH_KEY, "FLAT").build())
        );
        assertEquals("staged", DslQuerySettings.LaunchMode.STAGED.settingValue());
        assertEquals("flat", DslQuerySettings.LaunchMode.FLAT.settingValue());
    }

    /**
     * Fail secure on a typo: an unknown value is <b>rejected</b>, never silently resolved to an arm. A
     * fallback would make a benchmark cell labelled {@code flat} report staged numbers, which is worse than
     * a rejected PUT because nothing downstream could tell.
     */
    public void testFanoutLaunchRejectsAnUnknownMode() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DslQuerySettings.FANOUT_LAUNCH.get(Settings.builder().put(FANOUT_LAUNCH_KEY, "staggered").build())
        );
        assertTrue("the rejected value must be named, got: " + e.getMessage(), e.getMessage().contains("staggered"));
        assertTrue("the valid values must be listed, got: " + e.getMessage(), e.getMessage().contains("staged, flat"));
    }

    /**
     * The node-settings value differs from the default ({@code staged}) on purpose: at the default this
     * test would also pass against a holder that ignored the node settings entirely — i.e. against an
     * {@code opensearch.yml} arm selection that silently did nothing.
     */
    public void testHolderReadsLaunchModeFromNodeSettings() {
        Settings nodeSettings = Settings.builder().put(FANOUT_LAUNCH_KEY, "flat").build();
        DslQuerySettings holder = new DslQuerySettings(clusterService(nodeSettings));

        assertEquals(DslQuerySettings.LaunchMode.FLAT, holder.fanoutLaunch());
    }

    /** Staged-by-default: an unset key must not put a node on the experimental arm. */
    public void testHolderDefaultsToTheStagedLaunch() {
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY));

        assertEquals(DslQuerySettings.LaunchMode.STAGED, holder.fanoutLaunch());
    }

    /**
     * Both directions plus the fall-back, for the same reason the width knob asserts all three: a
     * one-directional test would pass against a consumer that could only ever switch the arm on, and
     * switching it back off is how an E5 run returns a node to the shipped shape without a restart.
     */
    public void testDynamicUpdateWritesVolatileLaunchModeBothWays() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        assertEquals("the shipped launch shape", DslQuerySettings.LaunchMode.STAGED, holder.fanoutLaunch());

        clusterSettings.applySettings(Settings.builder().put(FANOUT_LAUNCH_KEY, "flat").build());
        assertEquals("the update consumer must have written the volatile", DslQuerySettings.LaunchMode.FLAT, holder.fanoutLaunch());

        clusterSettings.applySettings(Settings.builder().put(FANOUT_LAUNCH_KEY, "staged").build());
        assertEquals("switching back to the shipped arm must take effect", DslQuerySettings.LaunchMode.STAGED, holder.fanoutLaunch());

        clusterSettings.applySettings(Settings.builder().put(FANOUT_LAUNCH_KEY, "flat").build());
        assertEquals("switching arms again must take effect", DslQuerySettings.LaunchMode.FLAT, holder.fanoutLaunch());

        clusterSettings.applySettings(Settings.EMPTY);
        assertEquals("clearing the experiment must fall back to staged", DslQuerySettings.LaunchMode.STAGED, holder.fanoutLaunch());
    }

    /** Fail secure at the {@code _cluster/settings} layer too: a rejected arm must not partially apply. */
    public void testDynamicUpdateRejectsAnUnknownLaunchMode() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        clusterSettings.applySettings(Settings.builder().put(FANOUT_LAUNCH_KEY, "flat").build());
        assertEquals(DslQuerySettings.LaunchMode.FLAT, holder.fanoutLaunch());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(FANOUT_LAUNCH_KEY, "staggered").build())
        );
        assertTrue("expected the rejected key to be named, got: " + e.getMessage(), e.getMessage().contains(FANOUT_LAUNCH_KEY));
        assertEquals("the rejected value must not have been applied", DslQuerySettings.LaunchMode.FLAT, holder.fanoutLaunch());
    }

    /** A lost {@code Dynamic} silently turns the width knob into a restart-only one. */
    public void testSettingsAreNodeScopeAndDynamic() {
        for (Setting<?> setting : DslQuerySettings.all()) {
            assertTrue(setting.getKey() + " must be NodeScope", setting.hasNodeScope());
            assertTrue(setting.getKey() + " must be Dynamic", setting.isDynamic());
            assertFalse(setting.getKey() + " must not be IndexScope", setting.hasIndexScope());
            assertFalse(setting.getKey() + " must not be Final", setting.getProperties().contains(Setting.Property.Final));
        }
    }

    // ── The live holder ────────────────────────────────────────────────────

    /**
     * The node-settings value differs from the default ({@code 1}) on purpose: at the default this test
     * would also pass against a holder that ignored the node settings entirely.
     */
    public void testHolderReadsInitialValueFromNodeSettings() {
        Settings nodeSettings = Settings.builder().put(MAX_PARALLEL_KEY, 2).build();
        DslQuerySettings holder = new DslQuerySettings(clusterService(nodeSettings));

        assertEquals(2, holder.maxParallelSubPlans());
    }

    /** Sequential-by-default: an unset key must not widen the fan-out. */
    public void testHolderDefaultsToSequential() {
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY));

        assertEquals(1, holder.maxParallelSubPlans());
    }

    public void testDynamicUpdateWritesVolatileMaxParallelSubPlans() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        assertEquals("default before the update", 1, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());

        assertEquals("update consumer must have written the volatile", 2, holder.maxParallelSubPlans());
    }

    /**
     * A one-directional test would pass against a latch-style bug (a consumer that only ever widens),
     * so assert narrowing back and the fall-back-to-default too. Narrowing is the operator's rollback
     * path for the fan-out now that there is no separate {@code enabled} lever here: {@code 1} is
     * byte-identical to sequential execution.
     */
    public void testDynamicUpdateWritesVolatileMaxParallelSubPlansBothWays() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        assertEquals("the sequential default", 1, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());
        assertEquals("widening must take effect", 2, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 1).build());
        assertEquals("narrowing back to sequential must take effect", 1, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());
        assertEquals("widening again must take effect", 2, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.EMPTY);
        assertEquals("clearing the transient value must fall back to the default", 1, holder.maxParallelSubPlans());
    }

    /** Fail secure: a rejected update must not partially apply. */
    public void testDynamicUpdateRejectsThreeAtClusterSettingsLayer() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());
        assertEquals(2, holder.maxParallelSubPlans());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 3).build())
        );
        // The updater wraps the Setting's own parse failure; the bound is in the cause.
        assertTrue("expected the rejected key to be named, got: " + e.getMessage(), e.getMessage().contains(MAX_PARALLEL_KEY));
        assertNotNull("the Setting itself must be the thing that rejected 3", e.getCause());
        assertTrue(
            "expected an upper-bound message on the cause, got: " + e.getCause().getMessage(),
            e.getCause().getMessage().contains("must be <= 2")
        );
        assertEquals("the rejected value must not have been applied", 2, holder.maxParallelSubPlans());
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static List<String> keysOf(List<Setting<?>> settings) {
        return settings.stream().map(Setting::getKey).collect(Collectors.toList());
    }

    private static ClusterSettings registry() {
        return new ClusterSettings(Settings.EMPTY, Set.copyOf(DslQuerySettings.all()));
    }

    private static ClusterService clusterService(Settings nodeSettings) {
        return clusterService(nodeSettings, new ClusterSettings(nodeSettings, Set.copyOf(DslQuerySettings.all())));
    }

    private static ClusterService clusterService(Settings nodeSettings, ClusterSettings clusterSettings) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }
}
