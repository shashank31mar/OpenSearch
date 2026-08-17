/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.aggregation.GranularityKeys.GranularityLevel;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Assembles the {@code aggregations} section of a {@link org.opensearch.action.search.SearchResponse}
 * from the rows the analytics engine returned.
 *
 * <p>The join is <b>map-keyed on {@code QueryPlan.granularity()}</b>, never on list position: the
 * fan-out completes plans in whatever order the engine finishes them, so position carries no identity.
 * Each result's key is parsed <em>once</em> into its grouping levels
 * ({@link GranularityKeys#parseGranularityKey}); the parsed levels give the level's aggregation name —
 * which selects the result for a request-tree node — and its grouping field names — which name the
 * bucket-key columns. Nesting comes from the keys' prefix structure
 * ({@link GranularityKeys#directChildrenOf}), so a grandchild can never attach to a grandparent.
 *
 * <p>Two things are deliberately <b>not</b> reachable from here, and neither is an oversight:
 * <ul>
 *   <li>{@code AggregationMetadata} / {@code GroupingInfo} — converter-facing types that never travel to
 *       the response path. The parsed key carries the same group-by field-name list, by construction:
 *       {@code AggregationMetadataBuilder.build} concatenates exactly the same per-level lists in the
 *       same order.</li>
 *   <li>{@code RelOptUtil.toString()} / {@code RelNode.explain()} / {@code RelMetadataQuery} —
 *       assembly runs on the engine's completion thread, which never sets
 *       {@code RelMetadataQueryBase.THREAD_PROVIDERS}, so materialising a metadata query there throws
 *       NPE. Column names come from {@link ExecutionResult#getFieldNames()}, which is the plan's row type
 *       in the same order the rows are materialised.</li>
 * </ul>
 *
 * <p>Every inconsistency fails loudly. A duplicate granularity key, a requested aggregation with no
 * matching result and a corrupt key are all errors, not things to paper over: an empty bucket list is
 * indistinguishable from a legitimately empty result, and a positional fallback silently mis-joins.
 */
public final class AggregationAssembler {

    private static final Logger logger = LogManager.getLogger(AggregationAssembler.class);

    private final AggregationRegistry registry;

    /**
     * Creates an assembler.
     *
     * @param registry the translator registry used to build the bucket and metric leaves
     */
    public AggregationAssembler(AggregationRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
    }

    /**
     * Assembles the response's aggregations from the AGGREGATION results.
     *
     * @param results all execution results for the request; HITS results are ignored here
     * @param searchSource the original request body, which carries the aggregation builders the leaves
     *     need (bucket name, order, thresholds); may be null or carry no aggregations
     * @return the assembled aggregations, or null when the request asked for none — null is what
     *     {@code SearchResponseSections} takes to mean "no aggregations section"
     * @throws IllegalStateException if the results and the request disagree
     * @throws IllegalArgumentException if a plan's granularity key does not decode
     */
    public InternalAggregations assemble(List<ExecutionResult> results, SearchSourceBuilder searchSource) {
        Objects.requireNonNull(results, "results must not be null");
        Collection<AggregationBuilder> requested = requestedAggregations(searchSource);
        if (requested.isEmpty()) {
            return null;
        }

        Map<String, ExecutionResult> byGranularity = indexByGranularity(results);
        // Parsed once per key, not once per row: a 10k-row result re-parsing its own key per row is pure
        // waste on the completion thread.
        Map<String, List<GranularityLevel>> levelsByKey = new LinkedHashMap<>();
        for (String key : byGranularity.keySet()) {
            levelsByKey.put(key, GranularityKeys.parseGranularityKey(key));
        }

        Context context = new Context(byGranularity, levelsByKey);
        List<String> topLevelKeys = GranularityKeys.directChildrenOf(GranularityKeys.ROOT, byGranularity.keySet());

        List<InternalAggregation> assembled = new ArrayList<>();
        for (AggregationBuilder agg : requested) {
            if (context.translatorFor(registry, agg) instanceof BucketTranslator) {
                assembled.add(assembleBucket(agg, topLevelKeys, List.of(), context));
            } else {
                // A metric at the top level has no GROUP BY, so it lives at the ROOT granularity, whose
                // plan produces exactly one row.
                assembled.add(assembleUngroupedMetric(agg, context));
            }
        }
        return InternalAggregations.from(assembled);
    }

    /**
     * Builds one bucket aggregation and, recursively, everything nested under it.
     *
     * @param agg the bucket aggregation builder from the request
     * @param candidateKeys the granularity keys one level below the enclosing bucket
     * @param parentKeyValues the enclosing bucket's group-by column values, in group-by column order;
     *     empty at the top level
     */
    private InternalAggregation assembleBucket(
        AggregationBuilder agg,
        List<String> candidateKeys,
        List<Object> parentKeyValues,
        Context context
    ) {
        String granularity = selectByAggName(agg.getName(), candidateKeys, context);
        ExecutionResult result = context.byGranularity().get(granularity);
        List<GranularityLevel> levels = context.levelsByKey().get(granularity);

        List<String> groupByNames = groupByNamesOf(levels);
        List<String> ownKeyNames = levels.get(levels.size() - 1).fieldNames();
        if (ownKeyNames.isEmpty()) {
            throw new IllegalStateException("Aggregation [" + agg.getName() + "] has no grouping column in its granularity key");
        }
        // Per-granularity, NOT per-invocation: assembleBucket runs once per PARENT row for a nested
        // aggregation, so anything derived from the granularity alone has to be memoised or it is redone
        // for every parent bucket. See Context.
        ColumnIndex columns = context.columnsFor(granularity, agg.getName(), result.getFieldNames());

        List<AggregationBuilder> subBuckets = new ArrayList<>();
        List<AggregationBuilder> subMetrics = new ArrayList<>();
        for (AggregationBuilder sub : agg.getSubAggregations()) {
            if (context.translatorFor(registry, sub) instanceof BucketTranslator) {
                subBuckets.add(sub);
            } else {
                subMetrics.add(sub);
            }
        }
        List<String> childKeys = subBuckets.isEmpty() ? List.of() : context.directChildrenOf(granularity);

        // One pass over this granularity's rows, grouped by the parent bucket they belong under, instead of
        // a full re-scan filtered per parent row: that was O(parentRows x childRows) per nesting level on
        // the engine's completion thread.
        int parentPrefixLength = groupByNames.size() - ownKeyNames.size();
        Map<List<Object>, List<Object[]>> rowsByParent = context.rowsUnder(granularity, result, groupByNames, parentPrefixLength, columns);

        List<BucketEntry> entries = new ArrayList<>();
        for (Object[] row : rowsByParent.getOrDefault(parentKeyValues, List.of())) {
            List<Object> keyValues = new ArrayList<>(groupByNames.size());
            for (String name : groupByNames) {
                keyValues.add(row[columns.of(name)]);
            }
            List<Object> ownKeys = keyValues.subList(parentPrefixLength, groupByNames.size());

            List<InternalAggregation> nested = new ArrayList<>(subMetrics.size() + subBuckets.size());
            for (AggregationBuilder metric : subMetrics) {
                MetricTranslator<AggregationBuilder> metricTranslator = registry.getMetric(metric.getClass());
                nested.add(metricTranslator.toInternalAggregation(metric.getName(), row[columns.of(metric.getName())]));
            }
            for (AggregationBuilder child : subBuckets) {
                nested.add(assembleBucket(child, childKeys, keyValues, context));
            }

            long docCount = docCountOf(agg.getName(), row, columns);
            // unmodifiableList, not List.copyOf: a grouping column value can legitimately be null and
            // List.copyOf rejects null elements.
            entries.add(
                new BucketEntry(Collections.unmodifiableList(new ArrayList<>(ownKeys)), docCount, InternalAggregations.from(nested))
            );
        }

        BucketTranslator<AggregationBuilder> bucketTranslator = registry.getBucket(agg.getClass());
        return bucketTranslator.toBucketAggregation(agg, entries);
    }

    /** Builds a top-level metric: no GROUP BY, so the ROOT granularity's single row holds its value. */
    private InternalAggregation assembleUngroupedMetric(AggregationBuilder agg, Context context) {
        ExecutionResult result = context.byGranularity().get(GranularityKeys.ROOT);
        if (result == null) {
            throw new IllegalStateException(
                "No execution result for aggregation [" + agg.getName() + "] at the root (no GROUP BY) granularity"
            );
        }
        ColumnIndex columns = new ColumnIndex(agg.getName(), result.getFieldNames());
        MetricTranslator<AggregationBuilder> metricTranslator = registry.getMetric(agg.getClass());

        for (Object[] row : result.getRows()) {
            return metricTranslator.toInternalAggregation(agg.getName(), row[columns.of(agg.getName())]);
        }
        // A no-GROUP-BY aggregate always produces one row, so an empty result means the executor returned
        // nothing for a plan it reported successful. The metric's own empty convention is the honest
        // rendering — the same one the plan would have produced over an empty index — never 0.
        return metricTranslator.toInternalAggregation(agg.getName(), null);
    }

    /**
     * Selects the granularity whose deepest level was contributed by {@code aggName}. Selection is by
     * the parsed aggregation-name path, not by the bucket's field name: two aggregations over the same
     * field are different granularities and must resolve to different results.
     */
    private static String selectByAggName(String aggName, List<String> candidateKeys, Context context) {
        String found = null;
        for (String key : candidateKeys) {
            List<GranularityLevel> levels = context.levelsByKey().get(key);
            if (levels.get(levels.size() - 1).aggName().equals(aggName)) {
                if (found != null) {
                    throw new IllegalStateException(
                        "Two granularities claim aggregation [" + aggName + "]: [" + found + "] and [" + key + "]"
                    );
                }
                found = key;
            }
        }
        if (found == null) {
            throw new IllegalStateException(
                "No execution result for aggregation [" + aggName + "]; the executor produced no plan for its granularity"
            );
        }
        return found;
    }

    /**
     * Indexes the AGGREGATION results by granularity key.
     *
     * <p>A duplicate key is an upstream bug — before the aggregation name entered the key, every
     * same-field sibling pair produced one — so it throws rather than letting the second result
     * overwrite the first.
     */
    private static Map<String, ExecutionResult> indexByGranularity(List<ExecutionResult> results) {
        Map<String, ExecutionResult> byGranularity = new LinkedHashMap<>();
        for (ExecutionResult result : results) {
            if (result.getType() != QueryPlans.Type.AGGREGATION) {
                continue;
            }
            String key = result.getPlan().granularity();
            ExecutionResult previous = byGranularity.put(key, result);
            if (previous != null) {
                throw new IllegalStateException("Two execution results share the granularity key [" + key + "]");
            }
        }
        return byGranularity;
    }

    /**
     * The group-by column names for a granularity, in column order. Identical by construction to
     * {@code AggregationMetadata.getGroupByFieldNames()} — that list is the same per-level concatenation
     * over the same groupings the key was built from.
     */
    private static List<String> groupByNamesOf(List<GranularityLevel> levels) {
        List<String> names = new ArrayList<>();
        for (GranularityLevel level : levels) {
            names.addAll(level.fieldNames());
        }
        return names;
    }

    /**
     * Groups a granularity's rows by the enclosing bucket's group-by values — the leading
     * {@code prefixLength} group-by columns, which are exactly the parent granularity's group-by columns
     * because a child key extends its parent's key by one level.
     *
     * <p>Row order inside each group is the result's own row order, so bucket construction stays as
     * deterministic as the previous filter-in-place scan was.
     */
    private static Map<List<Object>, List<Object[]>> groupRowsByParent(
        ExecutionResult result,
        List<String> groupByNames,
        int prefixLength,
        ColumnIndex columns
    ) {
        Map<List<Object>, List<Object[]>> byParent = new LinkedHashMap<>();
        for (Object[] row : result.getRows()) {
            // A grouping column value can legitimately be null, so this cannot be List.of/List.copyOf.
            List<Object> prefix = new ArrayList<>(prefixLength);
            for (int i = 0; i < prefixLength; i++) {
                prefix.add(row[columns.of(groupByNames.get(i))]);
            }
            byParent.computeIfAbsent(Collections.unmodifiableList(prefix), unused -> new ArrayList<>()).add(row);
        }
        return byParent;
    }

    private static long docCountOf(String aggName, Object[] row, ColumnIndex columns) {
        Object value = row[columns.of(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME)];
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException(
            "Aggregation ["
                + aggName
                + "] doc_count column ["
                + AggregationMetadataBuilder.IMPLICIT_COUNT_NAME
                + "] must be a number, but was "
                + (value == null ? "null" : value.getClass().getName())
        );
    }

    private static Collection<AggregationBuilder> requestedAggregations(SearchSourceBuilder searchSource) {
        if (searchSource == null || searchSource.aggregations() == null || searchSource.aggregations().getAggregatorFactories() == null) {
            return List.of();
        }
        return searchSource.aggregations().getAggregatorFactories();
    }

    /**
     * Per-request assembly state: the results by key, their keys' parsed levels, and the per-granularity
     * derivations that must not be recomputed per parent row.
     *
     * <p>Single-threaded by construction — one request's assembly runs on one completion thread — so the
     * caches are plain {@code HashMap}s with no synchronisation.
     */
    private static final class Context {

        private final Map<String, ExecutionResult> byGranularity;
        private final Map<String, List<GranularityLevel>> levelsByKey;
        private final Map<String, ColumnIndex> columnsByGranularity = new HashMap<>();
        private final Map<String, List<String>> childrenByGranularity = new HashMap<>();
        private final Map<String, Map<List<Object>, List<Object[]>>> rowsByGranularity = new HashMap<>();

        Context(Map<String, ExecutionResult> byGranularity, Map<String, List<GranularityLevel>> levelsByKey) {
            this.byGranularity = byGranularity;
            this.levelsByKey = levelsByKey;
        }

        Map<String, ExecutionResult> byGranularity() {
            return byGranularity;
        }

        Map<String, List<GranularityLevel>> levelsByKey() {
            return levelsByKey;
        }

        /** The column index over one granularity's row type, built once. */
        ColumnIndex columnsFor(String granularity, String aggName, List<String> fieldNames) {
            return columnsByGranularity.computeIfAbsent(granularity, unused -> new ColumnIndex(aggName, fieldNames));
        }

        /**
         * The granularity keys one level below {@code granularity}, resolved once.
         * {@link GranularityKeys#directChildrenOf} re-parses every key in the set on each call, so calling
         * it once per parent row re-parsed the whole forest per bucket.
         */
        List<String> directChildrenOf(String granularity) {
            return childrenByGranularity.computeIfAbsent(granularity, key -> GranularityKeys.directChildrenOf(key, byGranularity.keySet()));
        }

        /** This granularity's rows grouped by their parent bucket's group-by values, grouped once. */
        Map<List<Object>, List<Object[]>> rowsUnder(
            String granularity,
            ExecutionResult result,
            List<String> groupByNames,
            int prefixLength,
            ColumnIndex columns
        ) {
            return rowsByGranularity.computeIfAbsent(granularity, unused -> groupRowsByParent(result, groupByNames, prefixLength, columns));
        }

        AggregationTranslator<?> translatorFor(AggregationRegistry registry, AggregationBuilder agg) {
            AggregationTranslator<?> translator = registry.get(agg.getClass());
            if (translator == null) {
                throw new IllegalStateException(
                    "No translator registered for aggregation [" + agg.getName() + "] of type " + agg.getClass().getSimpleName()
                );
            }
            return translator;
        }
    }

    /**
     * Column lookup by name over one plan's row type. Resolution is by name, never by position:
     * {@code AggregationMetadataBuilder.build} appends the implicit COUNT <em>after</em> the metric
     * calls, and group-by columns lead only as an artefact of how the aggregate is built — a positional
     * assumption is a guess wearing a comment.
     *
     * <p>A repeated column name is rejected rather than resolved to its first occurrence. Name-based
     * resolution is only sound while names are unique: keeping the first {@code _count} would bind
     * {@code doc_count} to a user metric that happens to be named {@code _count}, and every bucket would
     * report that metric's value as its document count — a silently wrong number, which is worse than a
     * failed request. {@code AggregationMetadataBuilder.build} rejects the colliding request up front, so
     * reaching this throw means the plan's row type was built some other way.
     *
     * <p><b>Where the row type goes when one of these throws fires.</b> Both throws below are internal
     * invariant violations whose diagnosis needs the whole row type, and both messages reach the client:
     * {@code TransportDslExecuteAction}'s build-failure arm hands the exception to {@code onFailure}, and
     * OpenSearch renders an {@code IllegalStateException}'s message into the error body. So the full row
     * type is <em>logged</em> here and the thrown message names only the aggregation plus the one
     * offending column — the operator keeps the detail, the caller gets a message that enumerates no
     * other column of the index it queried.
     */
    private static final class ColumnIndex {

        private final String aggName;
        private final Map<String, Integer> byName;

        ColumnIndex(String aggName, List<String> fieldNames) {
            this.aggName = aggName;
            this.byName = new LinkedHashMap<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                Integer previous = byName.put(fieldNames.get(i), i);
                if (previous != null) {
                    logger.error(
                        "Aggregation [{}] cannot resolve columns by name: the plan's row type {} repeats column [{}]",
                        aggName,
                        fieldNames,
                        fieldNames.get(i)
                    );
                    throw new IllegalStateException(
                        "Aggregation ["
                            + aggName
                            + "] cannot resolve columns by name: the plan's row type repeats column ["
                            + fieldNames.get(i)
                            + "]"
                    );
                }
            }
        }

        int of(String name) {
            Integer index = byName.get(name);
            if (index == null) {
                logger.error("Aggregation [{}] expects column [{}] but the plan's row type has {}", aggName, name, byName.keySet());
                throw new IllegalStateException(
                    "Aggregation [" + aggName + "] expects column [" + name + "], which the plan's row type does not provide"
                );
            }
            return index;
        }
    }
}
