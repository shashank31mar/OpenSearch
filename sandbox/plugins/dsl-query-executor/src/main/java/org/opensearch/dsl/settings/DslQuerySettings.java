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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Cluster-level operator knobs for the DSL sub-plan fan-out — its width, and which launch shape it
 * uses — plus a live holder for their values.
 * <p>
 * Both settings are {@code NodeScope} + {@code Dynamic}, so they are settable in
 * {@code opensearch.yml} and updatable at runtime through {@code _cluster/settings}. The holder
 * reads each value once at construction and keeps it current with an
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

    /**
     * The launch shape of a multi-plan DSL query — <b>experimental, default {@code staged}, and a
     * measurement tool rather than a feature.</b>
     * <p>
     * It exists to run the design's experiment <b>E5 ("cold/warm x FLAT vs STAGED launch")</b>, which has
     * never been run because only the staged shape was ever implemented. {@code staged} reproduces that
     * shape exactly, so the default is not a behaviour change; {@code flat} is the arm under measurement
     * and must not be turned on outside a benchmark or a deliberate experiment.
     * <p>
     * What the arms mean at the decision site
     * ({@code DslQueryPlanExecutor}):
     * <ul>
     *   <li>{@code staged} — plan 0 runs <i>alone</i> to warm the data node's parquet metadata cache, and
     *       only after it succeeds do the remaining {@code n - 1} plans go through the permit gate. The
     *       width therefore cannot exceed {@code n - 1}, so a query of exactly 2 plans — a 2-level nested
     *       aggregation with {@code size: 0}, i.e. the measured production shape — can never run anything
     *       concurrently.</li>
     *   <li>{@code flat} — all {@code n} plans go through the gate from the start, so the width can reach
     *       {@code n}. That is the whole point of the arm, and it is also its cost: no plan warms the
     *       metadata cache first, so on a cold node all {@code K_eff} plans can run <i>unbudgeted</i> on
     *       the data node at once. Staging exists to lower the expected number of concurrent unbudgeted
     *       queries; {@code flat} deliberately gives that up in order to measure what it buys.</li>
     * </ul>
     * Unlike the width knob there is no numeric cap to enforce here: an unrecognised value is rejected by
     * {@link LaunchMode#parse} (HTTP 400), never silently treated as one of the arms — a launch mode that
     * defaulted on a typo would attribute one arm's numbers to the other.
     */
    public static final Setting<LaunchMode> FANOUT_LAUNCH = new Setting<>(
        "dsl.query.fanout_launch",
        LaunchMode.STAGED.settingValue(),
        LaunchMode::parse,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The two launch shapes of {@link #FANOUT_LAUNCH}. Experimental: {@link #FLAT} exists so design
     * experiment E5 can be measured, and {@link #STAGED} is the shipped default whose behaviour is
     * unchanged by this enum existing.
     */
    public enum LaunchMode {
        /** Plan 0 alone first, then {@code n - 1} plans through the gate. The default, and today's shape. */
        STAGED,
        /** All {@code n} plans through the gate at once. The E5 measurement arm; never a default. */
        FLAT;

        /**
         * The lower-case token an operator writes and every reader of the width line sees. Derived from
         * the constant name rather than carried as a field, so a new arm cannot ship with a token that
         * disagrees with its name.
         *
         * @return this mode's setting value, e.g. {@code "staged"}
         */
        public String settingValue() {
            return name().toLowerCase(Locale.ROOT);
        }

        /**
         * Parses an operator-supplied value. Case-insensitive because {@code _cluster/settings} values are
         * hand-typed, but an unknown value <b>throws</b> rather than falling back to a default: with a
         * silent fallback a typo in the {@code flat} arm of a benchmark would produce staged numbers
         * labelled as flat, which is worse than a rejected PUT.
         *
         * @param raw the raw setting value
         * @return the matching mode
         * @throws IllegalArgumentException if {@code raw} names no mode
         */
        static LaunchMode parse(String raw) {
            for (LaunchMode mode : values()) {
                if (mode.settingValue().equalsIgnoreCase(raw)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException(
                "unknown DSL fan-out launch mode ["
                    + raw
                    + "]; expected one of ["
                    + Arrays.stream(values()).map(LaunchMode::settingValue).collect(Collectors.joining(", "))
                    + "]"
            );
        }
    }

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
     * @return the SC-1 width descriptor plus the experimental launch-mode descriptor
     */
    public static List<Setting<?>> all() {
        return List.of(MAX_PARALLEL_SUB_PLANS, FANOUT_LAUNCH);
    }

    // Volatile: the update consumers run on the cluster-applier thread while readers are on SEARCH threads.
    private volatile int maxParallelSubPlans;
    private volatile LaunchMode fanoutLaunch;

    /**
     * Reads the settings from the node settings and registers update consumers so later
     * {@code _cluster/settings} changes are visible to readers without a restart.
     *
     * @param clusterService supplies the node settings and the {@link ClusterSettings} registry
     */
    public DslQuerySettings(ClusterService clusterService) {
        this.maxParallelSubPlans = MAX_PARALLEL_SUB_PLANS.get(clusterService.getSettings());
        this.fanoutLaunch = FANOUT_LAUNCH.get(clusterService.getSettings());
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(MAX_PARALLEL_SUB_PLANS, v -> maxParallelSubPlans = v);
        clusterSettings.addSettingsUpdateConsumer(FANOUT_LAUNCH, v -> fanoutLaunch = v);
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

    /**
     * Current value of {@code dsl.query.fanout_launch} — which launch shape the next multi-plan query
     * takes. Read once per query at the top of {@code DslQueryPlanExecutor.execute}, not per plan: a
     * dynamic update landing mid-query must not split one query across two launch shapes.
     *
     * @return the configured launch mode, never null
     */
    public LaunchMode fanoutLaunch() {
        return fanoutLaunch;
    }
}
