/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Translates a {@link TermsAggregationBuilder} — single-field GROUP BY.
 * {@code {"aggs": {"by_brand": {"terms": {"field": "brand"}}}}} becomes {@code GROUP BY brand}.
 */
public class TermsBucketTranslator implements BucketTranslator<TermsAggregationBuilder> {

    /** Creates a terms bucket translator. */
    public TermsBucketTranslator() {}

    @Override
    public Class<TermsAggregationBuilder> getAggregationType() {
        return TermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(TermsAggregationBuilder agg) {
        return new FieldGrouping(agg.getName(), List.of(agg.field()));
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(TermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(TermsAggregationBuilder agg) {
        return agg.order();
    }

    /**
     * {@inheritDoc}
     *
     * <p>The concrete {@code Terms} implementation follows the bucket key's runtime type, which is the
     * grouping column's type as the engine returned it: text keys yield {@link StringTerms}, integral
     * keys {@link LongTerms}, floating-point keys {@link DoubleTerms}.
     *
     * <p>Buckets are sorted to match {@code agg.order()}, then {@code min_doc_count} and {@code size} are
     * applied <b>coordinator-side</b>, and this is exact rather than approximate: neither threshold is
     * pushed down, so the {@code GROUP BY} handed here is the <em>complete</em> bucket set for the query —
     * nothing was discarded shard-side that could make the top-{@code size} selection wrong. Not applying
     * them returned every distinct term, so a {@code terms} agg over a 500-value field answered with 500
     * buckets where the requested {@code size} was 10. Because the set is complete, {@code otherDocCount}
     * is the exact document total of the buckets {@code size} pushed out, and {@code docCountError} stays
     * {@code 0} — there is no shard-level truncation for it to bound. {@code min_doc_count} exclusions do
     * not feed {@code otherDocCount}, matching {@code InternalTerms.reduce}, which only accumulates the
     * buckets its size-bounded queue overflows.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> entries = new ArrayList<>();
        for (BucketEntry entry : buckets) {
            if (entry.keys() == null || entry.keys().size() != 1) {
                throw new IllegalStateException(
                    "terms aggregation ["
                        + agg.getName()
                        + "] expects exactly one bucket key per entry, but got "
                        + (entry.keys() == null ? "null" : String.valueOf(entry.keys().size()))
                );
            }
            // Dropped before the bucket is ever built, and deliberately not counted into otherDocCount.
            if (entry.docCount() >= agg.minDocCount()) {
                entries.add(entry);
            }
        }

        // reduceOrder mirrors the requested order: this response is coordinator-final (rendered straight
        // to XContent by SearchResponseBuilder, never reduced), so nothing reads reduceOrder — claiming a
        // different one would be a statement about a reduce that does not happen.
        BucketOrder order = agg.order();
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(
            agg.minDocCount(),
            agg.shardMinDocCount(),
            agg.size(),
            agg.shardSize() > 0 ? agg.shardSize() : agg.size()
        );
        int keptBuckets = Math.min(agg.size(), entries.size());

        return switch (keyKindOf(entries)) {
            case LONG -> {
                List<LongTerms.Bucket> longBuckets = new ArrayList<>(entries.size());
                for (BucketEntry entry : entries) {
                    longBuckets.add(
                        new LongTerms.Bucket(
                            ((Number) entry.keys().get(0)).longValue(),
                            entry.docCount(),
                            subAggsOf(entry),
                            agg.showTermDocCountError(),
                            0L,
                            DocValueFormat.RAW
                        )
                    );
                }
                longBuckets.sort(order.comparator());
                long otherDocCount = trimToSize(longBuckets, keptBuckets);
                yield new LongTerms(
                    agg.getName(),
                    order,
                    order,
                    null,
                    DocValueFormat.RAW,
                    thresholds.getShardSize(),
                    agg.showTermDocCountError(),
                    otherDocCount,
                    longBuckets,
                    0L,
                    thresholds
                );
            }
            case DOUBLE -> {
                List<DoubleTerms.Bucket> doubleBuckets = new ArrayList<>(entries.size());
                for (BucketEntry entry : entries) {
                    doubleBuckets.add(
                        new DoubleTerms.Bucket(
                            ((Number) entry.keys().get(0)).doubleValue(),
                            entry.docCount(),
                            subAggsOf(entry),
                            agg.showTermDocCountError(),
                            0L,
                            DocValueFormat.RAW
                        )
                    );
                }
                doubleBuckets.sort(order.comparator());
                long otherDocCount = trimToSize(doubleBuckets, keptBuckets);
                yield new DoubleTerms(
                    agg.getName(),
                    order,
                    order,
                    null,
                    DocValueFormat.RAW,
                    thresholds.getShardSize(),
                    agg.showTermDocCountError(),
                    otherDocCount,
                    doubleBuckets,
                    0L,
                    thresholds
                );
            }
            case STRING -> {
                List<StringTerms.Bucket> stringBuckets = new ArrayList<>(entries.size());
                for (BucketEntry entry : entries) {
                    Object key = entry.keys().get(0);
                    stringBuckets.add(
                        new StringTerms.Bucket(
                            new BytesRef(key == null ? "" : key.toString()),
                            entry.docCount(),
                            subAggsOf(entry),
                            agg.showTermDocCountError(),
                            0L,
                            DocValueFormat.RAW
                        )
                    );
                }
                stringBuckets.sort(order.comparator());
                long otherDocCount = trimToSize(stringBuckets, keptBuckets);
                yield new StringTerms(
                    agg.getName(),
                    order,
                    order,
                    null,
                    DocValueFormat.RAW,
                    thresholds.getShardSize(),
                    agg.showTermDocCountError(),
                    otherDocCount,
                    stringBuckets,
                    0L,
                    thresholds
                );
            }
        };
    }

    /**
     * Trims an already-ordered bucket list to the requested {@code size}, in place, and returns the exact
     * document total of the buckets it removed — {@code sum_other_doc_count}. Exact because the list is the
     * complete bucket set for the query (no push-down, so no shard-side discard), which is also why the
     * caller reports a {@code docCountError} of {@code 0}.
     */
    private static long trimToSize(List<? extends Terms.Bucket> ordered, int keptBuckets) {
        long otherDocCount = 0;
        for (int i = keptBuckets; i < ordered.size(); i++) {
            otherDocCount += ordered.get(i).getDocCount();
        }
        ordered.subList(keptBuckets, ordered.size()).clear();
        return otherDocCount;
    }

    /** The three bucket-key representations OpenSearch's terms aggregation has. */
    private enum KeyKind {
        STRING,
        LONG,
        DOUBLE
    }

    /**
     * Picks the representation from the first non-null key. A key column is single-typed by
     * construction — it is one Calcite column — so the first non-null key decides for all of them, and
     * an all-null (or empty) bucket list falls back to STRING, whose key rendering is total.
     */
    private static KeyKind keyKindOf(List<BucketEntry> entries) {
        for (BucketEntry entry : entries) {
            Object key = entry.keys().get(0);
            if (key == null) {
                continue;
            }
            if (key instanceof Byte || key instanceof Short || key instanceof Integer || key instanceof Long) {
                return KeyKind.LONG;
            }
            if (key instanceof Float || key instanceof Double) {
                return KeyKind.DOUBLE;
            }
            return KeyKind.STRING;
        }
        return KeyKind.STRING;
    }

    private static InternalAggregations subAggsOf(BucketEntry entry) {
        return entry.subAggs() == null ? InternalAggregations.EMPTY : entry.subAggs();
    }
}
