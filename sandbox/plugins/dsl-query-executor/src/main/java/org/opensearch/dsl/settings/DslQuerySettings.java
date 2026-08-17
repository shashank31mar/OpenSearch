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

import java.util.List;

/**
 * Cluster-level operator knob for the DSL sub-plan fan-out width, plus a live holder for its value.
 * <p>
 * The setting is {@code NodeScope} + {@code Dynamic}, so it is settable in
 * {@code opensearch.yml} and updatable at runtime through {@code _cluster/settings}. The holder
 * reads the value once at construction and keeps it current with an
 * {@link ClusterSettings#addSettingsUpdateConsumer(Setting, java.util.function.Consumer)}
 * registration, so query-path readers pay a single volatile read rather than re-parsing the
 * {@code Settings} map.
 */
public final class DslQuerySettings {

    /**
     * Maximum number of sub-plans a single DSL query may execute concurrently ("K_setting").
     * Default 1, which is byte-identical to sequential execution; the hard maximum is <b>2</b>.
     * <p>
     * The maximum is enforced by this {@code Setting} itself — a {@code _cluster/settings} PUT of
     * {@code 3} is rejected with an {@link IllegalArgumentException} (HTTP 400), not silently
     * clamped downstream. That matters because a query holds at most {@code F} execution fragments
     * per node when it runs sequentially but {@code K_eff x F} when it fans out, and this cap is
     * the only bound on that multiplication. The same cap is re-applied inside the effective-K
     * {@code min} at the fan-out decision site, so relaxing it here alone does not widen the fan-out.
     * <p>
     * Why 2 and not 3: at a 3-plan query ({@code waves = 1 + ceil((n-1)/min(K, n-1))}) K=2 and K=3
     * both model out at 1.50x / 2 waves — after plan 0 runs alone only 2 plans remain, so a wider
     * gate has nothing left to admit. K=3 would ask for 50% more concurrent plans (threads and
     * Arrow memory) for zero modelled gain. K&gt;2 first pays at 4 plans; raising this maximum is a
     * deliberate re-decision backed by a new measurement, not a config tweak.
     */
    public static final Setting<Integer> MAX_PARALLEL_SUB_PLANS = Setting.intSetting(
        "dsl.query.max_parallel_sub_plans",
        1,
        1,
        2,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // There is deliberately no dsl.query.enabled here. The interception gate is already owned by
    // SearchActionFilter.INTERCEPT_SEARCH_ENABLED ("dsl.query_executor.intercept_search.enabled",
    // default false), which is the lever that decides whether _search reaches the DSL path at all.
    // A second switch for one behaviour is a trap: two keys that disagree, and an operator rollback
    // that only half works. MAX_PARALLEL_SUB_PLANS below is a width knob, not an off switch —
    // INTERCEPT_SEARCH_ENABLED=false is the revert.

    /**
     * Every setting this plugin registers <b>on top of</b> the base plugin's own. {@code
     * DslQueryExecutorPlugin.getSettings()} appends this list to
     * {@code SearchActionFilter.INTERCEPT_SEARCH_ENABLED} — a setting missing from here is rejected in
     * {@code opensearch.yml}, invisible to {@code _cluster/settings} (a PUT of it is a 400), and not
     * resolvable by key from another plugin's classloader.
     *
     * @return the SC-1 setting descriptor
     */
    public static List<Setting<?>> all() {
        return List.of(MAX_PARALLEL_SUB_PLANS);
    }

    // Volatile: the update consumers run on the cluster-applier thread while readers are on SEARCH threads.
    private volatile int maxParallelSubPlans;

    /**
     * Reads the setting from the node settings and registers an update consumer so later
     * {@code _cluster/settings} changes are visible to readers without a restart.
     *
     * @param clusterService supplies the node settings and the {@link ClusterSettings} registry
     */
    public DslQuerySettings(ClusterService clusterService) {
        this.maxParallelSubPlans = MAX_PARALLEL_SUB_PLANS.get(clusterService.getSettings());
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(MAX_PARALLEL_SUB_PLANS, v -> maxParallelSubPlans = v);
    }

    /**
     * Current value of {@code dsl.query.max_parallel_sub_plans} — the "K_setting" term of the
     * fan-out width. Read this per query rather than caching it at construction time, otherwise the
     * value freezes for the life of the node and {@code Property.Dynamic} means nothing.
     *
     * @return the configured maximum concurrent sub-plans, always in [1, 2]
     */
    public int maxParallelSubPlans() {
        return maxParallelSubPlans;
    }
}
