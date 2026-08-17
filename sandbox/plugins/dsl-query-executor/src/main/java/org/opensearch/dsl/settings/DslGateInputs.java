/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.search.SearchService;

import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads the three cluster-settings inputs the sub-plan fan-out decision needs, and nothing else.
 * <p>
 * This class holds <b>no arithmetic</b>: it hands out values (and one absence signal) that the
 * fan-out site composes into its effective-K terms. Adding a {@code min}, a {@code ceil}, or any
 * expression combining two of these accessors belongs at that site, not here.
 *
 * <h2>Why the reads go through {@link ClusterSettings}, by key</h2>
 * Two of the three inputs are declared by <i>other</i> plugins:
 * <ul>
 *   <li>{@code datafusion.concurrency.fragment_executor_multiplier} belongs to
 *       {@code analytics-backend-datafusion}, which is a <b>sibling</b> plugin — it declares
 *       {@code extendedPlugins = ['analytics-engine']} exactly as this plugin does, so its classes
 *       are neither on this plugin's compile classpath nor reachable through the parent-classloader
 *       fallback. A typed reference to {@code DatafusionSettings} here would not compile, and making
 *       it compile would break this plugin's runtime classloader graph.</li>
 *   <li>{@code analytics.query.max_concurrent_shard_requests_per_node} belongs to
 *       {@code analytics-engine}, which is this plugin's <b>parent</b> ({@code extendedPlugins}) and
 *       <i>is</i> on the compile classpath — so a typed
 *       {@code AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE} reference <b>would</b>
 *       compile and <b>would</b> resolve at runtime. It is read untyped by key anyway, deliberately:
 *       one read mechanism for all three inputs, no compile-time coupling to another plugin's
 *       constant inside this class, and a rename on the {@code analytics-engine} side degrades to
 *       the documented fallback below instead of a {@code NoSuchFieldError}. Do not "simplify" this
 *       into an import and conclude the sibling case above was wrong — the two cases differ.</li>
 * </ul>
 * The key resolves across the plugin boundary because there is no classloader involved: {@code Node}
 * collects {@code pluginsService.getPluginSettings()} into its additional settings, {@code
 * SettingsModule}'s constructor registers each one, {@code registerSetting} puts every
 * {@code NodeScope} setting into one {@code nodeSettings} map, and the single {@link ClusterSettings}
 * instance is built from that map. The join key is a {@code String}.
 * <p>
 * Values are read from {@link ClusterSettings} on every call rather than from a {@code Settings}
 * snapshot. All four underlying settings are {@code Property.Dynamic}; a snapshot (as
 * {@code clusterService.getSettings()} returns) silently misses runtime updates, whereas
 * {@code ClusterSettings.get(Setting)} resolves the last applied cluster settings over the node
 * settings and is therefore always live.
 */
public final class DslGateInputs {

    private static final Logger logger = LogManager.getLogger(DslGateInputs.class);

    /**
     * Key of the sibling DataFusion plugin's concurrency-gate multiplier. Held as a string on
     * purpose — see the class javadoc; {@code DatafusionSettings} is not referenceable from here.
     */
    private static final String FRAGMENT_EXECUTOR_MULTIPLIER_KEY = "datafusion.concurrency.fragment_executor_multiplier";

    /** Key of the parent analytics-engine plugin's per-node in-flight shard-request cap. */
    private static final String MAX_CONCURRENT_SHARD_REQUESTS_KEY = "analytics.query.max_concurrent_shard_requests_per_node";

    private final ClusterSettings clusterSettings;

    // One-shot latches so the "input unavailable" cases are reported once per node, not per query.
    // One latch per distinct condition, deliberately: a latch shared between the "key not registered",
    // "registered with a non-numeric value" and "registered but unreadable" branches would let whichever
    // fired first suppress the others' messages for the life of the node, leaving the log describing the
    // wrong cause.
    private final AtomicBoolean multiplierUnregisteredLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierNonNumericLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierNonFiniteLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierNonPositiveLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierUnreadableLogged = new AtomicBoolean();
    private final AtomicBoolean shardRequestCapUnregisteredLogged = new AtomicBoolean();
    private final AtomicBoolean shardRequestCapNonNumericLogged = new AtomicBoolean();
    private final AtomicBoolean shardRequestCapUnreadableLogged = new AtomicBoolean();

    /**
     * Creates a reader over the live cluster-settings registry.
     *
     * @param clusterSettings the node's single {@link ClusterSettings} instance, holding every
     *                        {@code NodeScope} setting of every installed plugin
     */
    public DslGateInputs(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    /**
     * Live value of {@code datafusion.concurrency.fragment_executor_multiplier}, or empty when no
     * backend on this node declares it.
     * <p>
     * <b>Empty means the concurrency-gate term is dropped from the fan-out width — never that the
     * multiplier is {@code 1.0}.</b> {@code analytics-backend-lucene} is a real second backend with
     * no concurrency gate at all; treating its absent multiplier as {@code 1.0} and feeding that
     * through the gate term would pin the effective K at 1 there forever, a silent permanent kill of
     * the feature on that backend. The {@link OptionalDouble} return type exists so that a caller
     * cannot fall into a {@code 1.0} sentinel by accident: the consumer's "gate term present" flag
     * must be {@link OptionalDouble#isPresent()}, and when it is false the value is unread.
     * <p>
     * Reading the <i>descriptor</i> rather than duplicating DataFusion's {@code 1.5} default here
     * means the real default and any dynamic override both come from DataFusion's own
     * {@code Setting} — this half of the read path is drift-proof by construction.
     *
     * @return the multiplier, always &gt; 0 when present, or {@link OptionalDouble#empty()} if the key is
     *         unregistered, registered with a non-numeric type, registered with a non-finite or
     *         non-positive value, or registered with a value the owning {@code Setting} refuses to hand
     *         over
     */
    public OptionalDouble fragmentExecutorMultiplier() {
        Setting<?> descriptor = clusterSettings.get(FRAGMENT_EXECUTOR_MULTIPLIER_KEY);
        if (descriptor == null) {
            if (firstDebugReport(multiplierUnregisteredLogged)) {
                logger.debug(
                    "[{}] is not registered on this node (no gated backend installed); the concurrency-gate "
                        + "term is dropped from the sub-plan fan-out width",
                    FRAGMENT_EXECUTOR_MULTIPLIER_KEY
                );
            }
            return OptionalDouble.empty();
        }
        Object value;
        try {
            value = clusterSettings.get(descriptor);
        } catch (RuntimeException e) {
            // Registered but unreadable. The descriptor came out of this same registry, so a scope
            // mismatch cannot happen here — what can is the owning Setting refusing to parse the value it
            // has been given (AbstractScopedSettings.get(Setting) delegates straight to
            // setting.get(lastSettingsApplied, settings), which propagates the Setting's own
            // IllegalArgumentException). Same fail-secure "drop the term" signal as absent: this whole read
            // path exists to survive another plugin changing its setting under us, so it must not turn that
            // into a failed query.
            if (firstDebugReport(multiplierUnreadableLogged)) {
                logger.debug(
                    "[{}] is registered but its live value could not be read [{}]; the concurrency-gate term "
                        + "is dropped from the sub-plan fan-out width",
                    FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                    e
                );
            }
            return OptionalDouble.empty();
        }
        if (value instanceof Number number) {
            double multiplier = number.doubleValue();
            if (Double.isFinite(multiplier) == false) {
                // Numeric-typed but unusable, and reachable through the owning descriptor rather than only
                // through a hypothetical re-declaration: Setting's double parser range-checks with
                // value < min / value > max, and both comparisons are false for NaN, so an operator PUT of
                // "NaN" passes DataFusion's own 0.1-to-10.0 bounds. Handing that on would poison every term
                // derived from it and print as NaN in the fan-out's observability line, so it degrades to the
                // same absent signal as an unreadable value. Note what that costs and does not cost: the
                // gate term is dropped, so the fan-out falls back to being bounded by the sub-plan count,
                // the operator's own cap (max 2, enforced in the Setting) and the search-pool term — it is
                // NOT substituted with 1.0, the one value this contract forbids synthesising.
                if (firstDebugReport(multiplierNonFiniteLogged)) {
                    logger.debug(
                        "[{}] is registered with a non-finite value [{}]; the concurrency-gate term is dropped "
                            + "from the sub-plan fan-out width",
                        FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                        multiplier
                    );
                }
                return OptionalDouble.empty();
            }
            if (multiplier <= 0.0) {
                // Domain guard on a single value, the symmetric counterpart of the Math.max(1L, raw) in
                // maxConcurrentShardRequestsPerNode(): both defend the same hypothesis — a bounds relaxation
                // or a type change (doubleSetting -> intSetting with a 0 default) on the declaring side,
                // which is compile-invisible here precisely because the read is by string. A multiplier of 0
                // or below makes the consumer's gate term vCPU * multiplier / target_partitions zero or
                // negative, i.e. a permanently killed fan-out or a negative width — the direction this
                // class's contract says it must never fail into.
                // Dropped rather than clamped, unlike the cap above, because there is no floor to clamp to:
                // DataFusion's own 0.1 minimum would be a duplicated literal (the drift this untyped read
                // exists to avoid) and 1.0 is the one substitution this contract forbids. Unreachable on a
                // stock node today — the owning descriptor's bounds are 0.1 to 10.0 and applySettings
                // validates against them — the same "defence, not a live branch" status as the unregistered
                // cap above.
                if (firstDebugReport(multiplierNonPositiveLogged)) {
                    logger.debug(
                        "[{}] is registered with a non-positive value [{}]; the concurrency-gate term is dropped "
                            + "from the sub-plan fan-out width",
                        FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                        multiplier
                    );
                }
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(multiplier);
        }
        // Registered but not numeric: fail secure with the same "drop the term" signal rather than
        // throwing a cast exception onto the query path.
        if (firstDebugReport(multiplierNonNumericLogged)) {
            logger.debug(
                "[{}] is registered with a non-numeric value [{}]; the concurrency-gate term is dropped "
                    + "from the sub-plan fan-out width",
                FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                value
            );
        }
        return OptionalDouble.empty();
    }

    /**
     * Live re-derivation of the backend's {@code target_partitions}, clamped to at least 1.
     * <p>
     * The backend's own derivation is {@code private static}, so this mirrors it from the same two
     * {@code :server} settings it reads. Both are in {@code ClusterSettings.BUILT_IN_CLUSTER_SETTINGS},
     * so these two reads are typed and can never be absent — only the sibling plugin's key needs the
     * untyped route. The clamp is here and not in {@link #deriveTargetPartitionsMirror} because
     * consumers divide by this value and the mirror can legitimately return 0 (see that method).
     *
     * @return the derived target partition count, always &gt;= 1
     */
    public int targetPartitions() {
        String mode = clusterSettings.get(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
        int maxSliceCount = clusterSettings.get(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        return Math.max(1, deriveTargetPartitionsMirror(mode, maxSliceCount));
    }

    /**
     * Live value of {@code analytics.query.max_concurrent_shard_requests_per_node}, clamped to at
     * least 1, or {@link Integer#MAX_VALUE} when the key cannot be resolved.
     * <p>
     * The fallback is {@code MAX_VALUE} and deliberately <b>not</b> the owning plugin's literal
     * default: duplicating that default here is exactly the drift this untyped read avoids. The
     * direction is also the fail-conservative one — {@code MAX_VALUE} turns the consumer's
     * {@code min} into a no-op, so the per-node fragment count falls back to the shard count on the
     * busiest node, which is the largest plausible value and therefore yields the <i>narrowest</i>
     * gate-derived fan-out. A hardcoded default that understated the real cap would widen it instead.
     * {@code MAX_VALUE} is only safe because the consumer <i>selects</i> it through a {@code min};
     * never add to or multiply it.
     * <p>
     * The unresolvable branch is unreachable on a running node — {@code extendedPlugins =
     * ['analytics-engine']} means this plugin refuses to load without the declaring plugin — so it
     * exists purely as defence against a rename on that side, which is compile-invisible here
     * precisely because the read is by string.
     *
     * @return the per-node in-flight shard-request cap, always &gt;= 1
     */
    public int maxConcurrentShardRequestsPerNode() {
        Setting<?> descriptor = clusterSettings.get(MAX_CONCURRENT_SHARD_REQUESTS_KEY);
        if (descriptor == null) {
            if (firstDebugReport(shardRequestCapUnregisteredLogged)) {
                logger.debug(
                    "[{}] is not registered on this node; treating the per-node shard-request cap as unbounded",
                    MAX_CONCURRENT_SHARD_REQUESTS_KEY
                );
            }
            return Integer.MAX_VALUE;
        }
        Object value;
        try {
            value = clusterSettings.get(descriptor);
        } catch (RuntimeException e) {
            // Registered but unreadable — see the same branch in fragmentExecutorMultiplier(). Falls back
            // to the documented MAX_VALUE rather than throwing onto the query path.
            if (firstDebugReport(shardRequestCapUnreadableLogged)) {
                logger.debug(
                    "[{}] is registered but its live value could not be read [{}]; treating the per-node "
                        + "shard-request cap as unbounded",
                    MAX_CONCURRENT_SHARD_REQUESTS_KEY,
                    e
                );
            }
            return Integer.MAX_VALUE;
        }
        if (value instanceof Number number) {
            // Domain guard on a single value, not composition: keeps the consumer's F >= 1 even if
            // the declaring plugin relaxes the setting's own minimum of 1. Saturating rather than
            // Number.intValue(): the read is by string, so a type change on the declaring side
            // (intSetting -> longSetting) is as invisible here as the rename the MAX_VALUE fallback
            // defends against, and intValue() would wrap a value above 2^31 into a small or negative
            // cap. That direction is the harmful one — the smallest cap yields the smallest F and
            // therefore the widest gate-derived fan-out, the opposite of the fallback's intent.
            long raw = number.longValue();
            return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, raw));
        }
        if (firstDebugReport(shardRequestCapNonNumericLogged)) {
            logger.debug(
                "[{}] is registered with a non-numeric value [{}]; treating the per-node shard-request cap as unbounded",
                MAX_CONCURRENT_SHARD_REQUESTS_KEY,
                value
            );
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Byte-faithful mirror of the DataFusion backend's {@code deriveTargetPartitions}: mode
     * {@code "none"} forces 1, a {@code maxSliceCount} of 0 means the backend owns the concurrency
     * level and uses half the available processors, otherwise the slice count is capped at the
     * available processors.
     * <p>
     * Keep this identical to the original <b>including the 0 it can return</b> on a 1-vCPU host with
     * {@code maxSliceCount == 0} — {@code TargetPartitionsDriftTests} pins the two against each
     * other, and "fixing" the 0 here would make that drift test meaningless. The divide-by-zero
     * hazard is handled by the clamp in {@link #targetPartitions()}.
     *
     * @param mode          value of {@code search.concurrent_segment_search.mode}
     * @param maxSliceCount value of {@code search.concurrent.max_slice_count}
     * @return the derived target partition count, which may be 0
     */
    static int deriveTargetPartitionsMirror(String mode, int maxSliceCount) {
        if (SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE.equals(mode)) {
            return 1;
        }

        if (maxSliceCount == 0) {
            return Runtime.getRuntime().availableProcessors() / 2;
        }

        return Math.min(maxSliceCount, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Trips a latch, returning {@code true} only the first time the message can actually be emitted.
     * These accessors run on every fan-out decision, so an unlatched log line would be per-query noise;
     * the log calls stay at their call sites (rather than behind a varargs helper) so the logger-usage
     * checker can see their arity.
     * <p>
     * The level check is <b>inside</b> the latch, not outside it. A latch that trips before the level is
     * consulted is consumed by the first fan-out decision on a node running at the default INFO, and the
     * diagnostic is then unreachable for the life of that node — so an operator who later raises
     * {@code logger.org.opensearch.dsl.settings} to DEBUG to find out why the fan-out is narrow would
     * never see the line explaining it. Short-circuiting on {@link Logger#isDebugEnabled()} means the
     * latch is only spent on a read that really logs.
     */
    private static boolean firstDebugReport(AtomicBoolean latch) {
        return logger.isDebugEnabled() && latch.compareAndSet(false, true);
    }
}
