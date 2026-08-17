/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.opensearch.be.datafusion.DatafusionSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drift guard for {@link DslGateInputs#deriveTargetPartitionsMirror(String, int)}.
 * <p>
 * The backend's own {@code deriveTargetPartitions} is {@code private static}, so this plugin has to
 * carry a copy of it. This test pins the copy against the real thing, reached through the backend's
 * public surface ({@code new DatafusionSettings(clusterService).getSnapshot().targetPartitions()}).
 * It is a same-JVM comparison of two pure functions — no cluster involved. The runtime half (that
 * the backend's settings are visible at all across the plugin boundary) is proven by
 * {@code DslQuerySettingsRestIT}.
 * <p>
 * {@code analytics-backend-datafusion} is on the <b>test</b> classpath only. Promoting it to
 * {@code compileOnly}/{@code implementation} would let {@code src/main} import
 * {@code DatafusionSettings} and re-introduce the sibling-classloader bug this workstream exists to
 * avoid; a test-scope dependency runs on the flat test classpath and changes nothing about the
 * plugin's runtime classloader graph.
 */
public class TargetPartitionsDriftTests extends OpenSearchTestCase {

    private static final String MODE_KEY = "search.concurrent_segment_search.mode";
    private static final String MAX_SLICE_COUNT_KEY = SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY;

    private static final String DRIFT_MESSAGE = "DslGateInputs.deriveTargetPartitionsMirror has drifted from "
        + "DatafusionSettings.deriveTargetPartitions — update the mirror (and re-check the fan-out's "
        + "A = vCPU * multiplier / target_partitions)";

    public void testMirrorMatchesDatafusionForModeNone() {
        for (int sliceCount : new int[] { 0, 1, 2, 8, 1024 }) {
            assertMirrorMatches(SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE, sliceCount);
        }
    }

    public void testMirrorMatchesDatafusionForModeAutoAndAll() {
        List<String> modes = List.of(SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO, SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL);
        int[] sliceCounts = new int[] { 0, 1, 2, 8, Runtime.getRuntime().availableProcessors(), 1024 };
        for (String mode : modes) {
            for (int sliceCount : sliceCounts) {
                assertMirrorMatches(mode, sliceCount);
            }
        }
    }

    /** The cell an operator actually runs: mode "auto" and the server's computed default slice count. */
    public void testMirrorMatchesDatafusionOnDefaults() {
        int expected = datafusionTargetPartitions(Settings.EMPTY);
        int mirrored = DslGateInputs.deriveTargetPartitionsMirror(
            SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.get(Settings.EMPTY),
            SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.get(Settings.EMPTY)
        );

        assertEquals(DRIFT_MESSAGE + " [defaults]", expected, mirrored);
    }

    /**
     * The tests above pin the derivation <i>function</i>; this pins the two settings it is <i>fed from</i>.
     * Both sides are driven off one {@link Settings} object, so a mirror that stayed byte-faithful while
     * {@link DslGateInputs#targetPartitions()} read a different key — or a hardcoded mode — would pass
     * every assertion above and fail here. Without this cell the drift guard covers only half of the copy:
     * the arithmetic, not the inputs.
     * <p>
     * The one permitted difference is the {@code >= 1} clamp the accessor applies (the backend's own
     * derivation returns 0 on a 1-vCPU host with {@code maxSliceCount == 0}, and consumers divide by the
     * value), so the expectation is {@code max(1, backend)} rather than the raw backend number.
     */
    public void testAccessorReadsTheSameTwoSettingsDatafusionDoes() {
        List<String> modes = List.of(
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO,
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL,
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE
        );
        for (String mode : modes) {
            for (int sliceCount : new int[] { 0, 1, 2, 8, 1024 }) {
                Settings nodeSettings = Settings.builder().put(MODE_KEY, mode).put(MAX_SLICE_COUNT_KEY, sliceCount).build();
                // Both concurrent-segment-search settings are in BUILT_IN_CLUSTER_SETTINGS, so the accessor's
                // typed reads resolve against a plain built-in registry.
                DslGateInputs inputs = new DslGateInputs(new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

                int expected = Math.max(1, datafusionTargetPartitions(nodeSettings));

                assertEquals(
                    String.format(Locale.ROOT, "%s [accessor, mode=%s sliceCount=%d]", DRIFT_MESSAGE, mode, sliceCount),
                    expected,
                    inputs.targetPartitions()
                );
            }
        }
    }

    private void assertMirrorMatches(String mode, int sliceCount) {
        Settings nodeSettings = Settings.builder().put(MODE_KEY, mode).put(MAX_SLICE_COUNT_KEY, sliceCount).build();

        int expected = datafusionTargetPartitions(nodeSettings);
        int mirrored = DslGateInputs.deriveTargetPartitionsMirror(mode, sliceCount);

        assertEquals(String.format(Locale.ROOT, "%s [mode=%s sliceCount=%d]", DRIFT_MESSAGE, mode, sliceCount), expected, mirrored);
    }

    /** The real derivation, reached through the only public route that exposes it. */
    private static int datafusionTargetPartitions(Settings nodeSettings) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(datafusionRegistry(nodeSettings));
        return new DatafusionSettings(clusterService).getSnapshot().targetPartitions();
    }

    /**
     * The settings {@code DatafusionSettings.registerListeners} attaches consumers to. Registration is
     * validated by identity, so the real descriptors have to be in the registry.
     */
    private static ClusterSettings datafusionRegistry(Settings nodeSettings) {
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(DatafusionSettings.BATCH_SIZE);
        registered.add(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS);
        registered.add(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS);
        registered.add(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT);
        registered.add(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD);
        registered.add(DatafusionSettings.INDEXED_FORCE_STRATEGY);
        return new ClusterSettings(nodeSettings, registered);
    }
}
