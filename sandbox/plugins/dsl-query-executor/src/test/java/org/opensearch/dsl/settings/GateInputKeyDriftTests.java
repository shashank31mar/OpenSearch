/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.be.datafusion.DatafusionSettings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * Drift guard for the two gate inputs {@link DslGateInputs} reads <b>untyped by key</b>.
 * <p>
 * {@code DslGateInputsTests} registers local descriptor <i>copies</i>, so it pins the read semantics
 * (absent is empty, never 1.0; a missing cap is {@code MAX_VALUE}, never a duplicated 5) but is blind
 * to the one failure that silently disables both reads in production: the owning plugin renaming its
 * key, changing its type, or dropping it from the list its plugin registers. Because the read is by
 * string that break is invisible at compile time, and its runtime symptom is not an error — the
 * multiplier degrades to "gate term dropped" and the cap to "unbounded" forever.
 * <p>
 * These tests therefore register the <b>real</b> descriptors, straight off the owning plugins'
 * constants, and assert that the accessors resolve them. Both plugins are on this plugin's
 * <b>test</b> classpath only; {@code src/main} still holds nothing but the two key strings, which is
 * what the whole untyped read path exists to preserve (see {@link DslGateInputs} class javadoc).
 * <p>
 * What this cannot prove is the runtime classloader graph — one shared {@code ClusterSettings} is
 * assembled here by hand rather than by {@code SettingsModule} from installed plugin zips. That half
 * is {@code DslQuerySettingsRestIT}'s (registration through real plugin zips) and
 * {@code DslQuerySettingsIT}'s (registration through a real node's {@code SettingsModule}).
 */
public class GateInputKeyDriftTests extends OpenSearchTestCase {

    private static final String DRIFT_HINT = " — DslGateInputs reads this key as a string; a rename, a type change or a"
        + " dropped registration is compile-invisible here and degrades silently at runtime";

    /**
     * The sibling backend's multiplier, read through the descriptor the backend itself declares. A
     * rename on that side turns this red instead of turning the fan-out's gate term permanently off.
     */
    public void testMultiplierReadResolvesTheRealDatafusionDescriptor() {
        DslGateInputs inputs = new DslGateInputs(registryWith(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER));

        OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();

        assertTrue(
            "the real datafusion.concurrency.fragment_executor_multiplier descriptor must resolve" + DRIFT_HINT,
            multiplier.isPresent()
        );
        assertEquals(OptionalDouble.of(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(Settings.EMPTY)), multiplier);
    }

    /** The live half: an override applied to the real descriptor's key must be visible to the read. */
    public void testMultiplierReadFollowsAnOverrideOfTheRealDescriptor() {
        ClusterSettings clusterSettings = registryWith(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.getKey(), 3.0).build());

        assertEquals(OptionalDouble.of(3.0), inputs.fragmentExecutorMultiplier());
    }

    /**
     * The parent plugin's per-node shard-request cap, read through the descriptor that plugin declares.
     * The expectation is the descriptor's <i>own</i> default, so this fails both if the key drifts (the
     * accessor would fall back to {@code MAX_VALUE}) and if the owner changes its default while this
     * plugin's documentation of it goes stale.
     */
    public void testShardRequestCapReadResolvesTheRealAnalyticsDescriptor() {
        DslGateInputs inputs = new DslGateInputs(registryWith(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE));

        int cap = inputs.maxConcurrentShardRequestsPerNode();

        assertNotEquals(
            "the real analytics.query.max_concurrent_shard_requests_per_node descriptor must resolve" + DRIFT_HINT,
            Integer.MAX_VALUE,
            cap
        );
        assertEquals((int) AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.get(Settings.EMPTY), cap);
    }

    /** The live half of the cap read, against the real descriptor. */
    public void testShardRequestCapReadFollowsAnOverrideOfTheRealDescriptor() {
        ClusterSettings clusterSettings = registryWith(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(
            Settings.builder().put(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey(), 2).build()
        );

        assertEquals(2, inputs.maxConcurrentShardRequestsPerNode());
    }

    /**
     * Both keys are only resolvable by string because their owners register them {@code NodeScope} —
     * that is what puts them in the one shared {@code nodeSettings} map {@code SettingsModule} builds
     * the node's {@code ClusterSettings} from. An owner switching either to {@code IndexScope} (or
     * adding {@code Final}) would make this plugin's read silently unresolvable / permanently frozen,
     * with no compile error and no runtime exception.
     */
    public void testBothForeignSettingsStayNodeScopeAndDynamic() {
        for (Setting<?> foreign : Set.of(
            DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER,
            AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE
        )) {
            assertTrue(foreign.getKey() + " must stay NodeScope to be resolvable by key" + DRIFT_HINT, foreign.hasNodeScope());
            assertTrue(foreign.getKey() + " must stay Dynamic — the reads are live, not snapshots", foreign.isDynamic());
            assertFalse(foreign.getKey() + " must not be IndexScope", foreign.hasIndexScope());
            assertFalse(foreign.getKey() + " must not be Final", foreign.getProperties().contains(Setting.Property.Final));
        }
    }

    /**
     * A descriptor that exists but is not in the list its plugin hands to {@code getSettings()} is never
     * registered on a node, so the key never resolves however correct the spelling is. Both owners
     * register through one list each, so that is the thing to pin.
     */
    public void testBothForeignSettingsAreStillRegisteredByTheirOwningPlugin() {
        assertTrue(
            "DataFusionPlugin.getSettings() returns DatafusionSettings.ALL_SETTINGS; the multiplier must be in it" + DRIFT_HINT,
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER)
        );
        assertTrue(
            "AnalyticsPlugin.getSettings() adds AnalyticsQuerySettings.all(); the shard-request cap must be in it" + DRIFT_HINT,
            AnalyticsQuerySettings.all().contains(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE)
        );
    }

    private static ClusterSettings registryWith(Setting<?> foreignSetting) {
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(foreignSetting);
        return new ClusterSettings(Settings.EMPTY, registered);
    }
}
