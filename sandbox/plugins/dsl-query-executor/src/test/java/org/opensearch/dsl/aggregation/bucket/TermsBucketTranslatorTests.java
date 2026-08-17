/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.metric.AvgMetricTranslator;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class TermsBucketTranslatorTests extends OpenSearchTestCase {

    private final TermsBucketTranslator translator = new TermsBucketTranslator();
    private final TermsAggregationBuilder brandAgg = new TermsAggregationBuilder("by_brand").field("brand");

    public void testGetGrouping() {
        assertEquals(List.of("brand"), translator.getGrouping(brandAgg).getFieldNames());
    }

    public void testGetSubAggregations() {
        TermsAggregationBuilder aggWithSub = new TermsAggregationBuilder("by_brand").field("brand")
            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        assertEquals(1, translator.getSubAggregations(aggWithSub).size());
    }

    public void testEmptySubAggregations() {
        assertTrue(translator.getSubAggregations(brandAgg).isEmpty());
    }

    public void testReportsCorrectType() {
        assertEquals(TermsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testGroupingReturnsFieldNameAsIs() {
        TermsAggregationBuilder badAgg = new TermsAggregationBuilder("by_bad").field("nonexistent");

        // Translator just captures the field name; validation happens at build time in the builder
        assertEquals(List.of("nonexistent"), translator.getGrouping(badAgg).getFieldNames());
    }

    public void testGetBucketOrderReturnsDefault() {
        // Default terms order is compound: _count desc, _key asc
        BucketOrder order = translator.getBucketOrder(brandAgg);
        assertNotNull(order);
        assertTrue(order instanceof InternalOrder.CompoundOrder);
        InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
        assertEquals(2, compound.orderElements().size());
        assertTrue(InternalOrder.isCountDesc(compound.orderElements().get(0)));
        assertTrue(InternalOrder.isKeyAsc(compound.orderElements().get(1)));
    }

    public void testGetBucketOrderReturnsCustomOrder() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(true));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // key(true) is already a key order — stored directly, not wrapped in CompoundOrder
        assertFalse(order instanceof InternalOrder.CompoundOrder);
        assertTrue(InternalOrder.isKeyOrder(order));
        assertTrue(InternalOrder.isKeyAsc(order));
    }

    public void testGetBucketOrderReturnsKeyDesc() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(false));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // key(false) is a key order — stored directly, not wrapped in CompoundOrder
        assertFalse(order instanceof InternalOrder.CompoundOrder);
        assertTrue(InternalOrder.isKeyOrder(order));
        assertFalse(InternalOrder.isKeyAsc(order));
    }

    public void testGetBucketOrderReturnsCountAsc() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(true));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // count(true) is not a key order — wrapped in CompoundOrder with _key asc tie-breaker
        assertTrue(order instanceof InternalOrder.CompoundOrder);
        InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
        assertEquals(2, compound.orderElements().size());
        assertEquals(BucketOrder.count(true), compound.orderElements().get(0));
        assertTrue(InternalOrder.isKeyAsc(compound.orderElements().get(1)));
    }

    public void testGetBucketOrderReturnsMetricOrder() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand")
            .order(BucketOrder.aggregation("avg_price", false));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // metric order is not a key order — wrapped in CompoundOrder with _key asc tie-breaker
        assertTrue(order instanceof InternalOrder.CompoundOrder);
        InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
        assertEquals(2, compound.orderElements().size());
        assertTrue(compound.orderElements().get(0) instanceof InternalOrder.Aggregation);
        assertTrue(InternalOrder.isKeyAsc(compound.orderElements().get(1)));
    }

    // ---- Response leaf (F3.2) ----
    // testToBucketAggregationNotYetImplemented is deliberately gone: it pinned the
    // UnsupportedOperationException this task replaces with a real implementation. That contract changed.

    public void testToBucketAggregationBuildsStringTerms() {
        InternalAggregation agg = translator.toBucketAggregation(brandAgg, List.of(bucket("BrandA", 3), bucket("BrandB", 2)));

        assertThat(agg, instanceOf(StringTerms.class));
        assertEquals("by_brand", agg.getName());
        List<? extends Terms.Bucket> buckets = ((StringTerms) agg).getBuckets();
        assertEquals(2, buckets.size());
        assertEquals("BrandA", buckets.get(0).getKeyAsString());
        assertEquals(3L, buckets.get(0).getDocCount());
        assertEquals("BrandB", buckets.get(1).getKeyAsString());
        assertEquals(2L, buckets.get(1).getDocCount());
    }

    public void testToBucketAggregationBuildsLongTermsForNumericKeys() {
        TermsAggregationBuilder priceAgg = new TermsAggregationBuilder("by_price").field("price").order(BucketOrder.key(true));
        InternalAggregation agg = translator.toBucketAggregation(priceAgg, List.of(bucket(700, 1), bucket(999L, 4)));

        assertThat(agg, instanceOf(LongTerms.class));
        List<? extends Terms.Bucket> buckets = ((LongTerms) agg).getBuckets();
        assertEquals(2, buckets.size());
        assertEquals(700L, buckets.get(0).getKeyAsNumber().longValue());
        assertEquals(999L, buckets.get(1).getKeyAsNumber().longValue());
    }

    public void testToBucketAggregationBuildsDoubleTermsForFloatingKeys() {
        TermsAggregationBuilder ratingAgg = new TermsAggregationBuilder("by_rating").field("rating").order(BucketOrder.key(true));
        InternalAggregation agg = translator.toBucketAggregation(ratingAgg, List.of(bucket(4.2, 1), bucket(4.5, 2)));

        assertThat(agg, instanceOf(DoubleTerms.class));
        assertEquals(4.2, ((DoubleTerms) agg).getBuckets().get(0).getKeyAsNumber().doubleValue(), 0.0);
    }

    public void testToBucketAggregationAppliesRequestedOrder() {
        TermsAggregationBuilder countDesc = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(false));
        // Fed in ascending doc_count order on purpose: the leaf must reorder, not preserve input order.
        InternalAggregation agg = translator.toBucketAggregation(countDesc, List.of(bucket("BrandB", 2), bucket("BrandA", 7)));

        List<? extends Terms.Bucket> buckets = ((StringTerms) agg).getBuckets();
        assertEquals("BrandA", buckets.get(0).getKeyAsString());
        assertEquals(7L, buckets.get(0).getDocCount());
        assertEquals("BrandB", buckets.get(1).getKeyAsString());
    }

    public void testToBucketAggregationCarriesSubAggregations() {
        InternalAggregations subAggs = InternalAggregations.from(
            List.of(new AvgMetricTranslator().toInternalAggregation("avg_price", 850.0))
        );
        InternalAggregation agg = translator.toBucketAggregation(brandAgg, List.of(new BucketEntry(List.of("BrandA"), 3L, subAggs)));

        Terms.Bucket bucket = ((StringTerms) agg).getBuckets().get(0);
        InternalAvg avg = bucket.getAggregations().get("avg_price");
        assertNotNull(avg);
        assertEquals(850.0, avg.getValue(), 0.0);
    }

    public void testToBucketAggregationOtherDocCountIsZeroWhenNothingWasDropped() {
        // Nothing was dropped shard-side and nothing exceeded the requested size, so a non-zero
        // sum_other_doc_count would be fabricated. doc_count_error_upper_bound stays 0 for good: the
        // coordinator sees the complete bucket set, so there is no shard-truncation error to bound.
        StringTerms agg = (StringTerms) translator.toBucketAggregation(brandAgg, List.of(bucket("BrandA", 3)));

        assertEquals(0L, agg.getSumOfOtherDocCounts());
        assertEquals(0L, agg.getDocCountError());
    }

    // Replaces testToBucketAggregationDoesNotDropBucketsBeyondRequestedSize: that pinned "never drop", which
    // returned every distinct term to a caller who asked for `size` of them. The engine applies neither
    // threshold, so the bucket set handed here is COMPLETE and applying them coordinator-side is exact.
    public void testToBucketAggregationDropsBucketsBeyondRequestedSize() {
        TermsAggregationBuilder sizeOne = new TermsAggregationBuilder("by_brand").field("brand").size(1);
        StringTerms agg = (StringTerms) translator.toBucketAggregation(sizeOne, List.of(bucket("BrandA", 3), bucket("BrandB", 2)));

        // Default order is _count desc, so BrandA is the one bucket that survives.
        assertEquals(1, agg.getBuckets().size());
        assertEquals("BrandA", agg.getBuckets().get(0).getKeyAsString());
        // Exact, not a guess: the dropped bucket's documents are the only ones missing.
        assertEquals(2L, agg.getSumOfOtherDocCounts());
        assertEquals(0L, agg.getDocCountError());
    }

    public void testToBucketAggregationTruncatesAfterOrderingNotBeforeIt() {
        // Fed worst-first: truncating input order would keep the wrong bucket.
        TermsAggregationBuilder sizeTwo = new TermsAggregationBuilder("by_brand").field("brand").size(2).order(BucketOrder.count(false));
        StringTerms agg = (StringTerms) translator.toBucketAggregation(
            sizeTwo,
            List.of(bucket("BrandC", 1), bucket("BrandB", 5), bucket("BrandA", 9))
        );

        assertEquals(
            List.of("BrandA", "BrandB"),
            List.of(agg.getBuckets().get(0).getKeyAsString(), agg.getBuckets().get(1).getKeyAsString())
        );
        assertEquals(1L, agg.getSumOfOtherDocCounts());
    }

    public void testToBucketAggregationDropsBucketsBelowMinDocCount() {
        TermsAggregationBuilder minThree = new TermsAggregationBuilder("by_brand").field("brand").minDocCount(3);
        StringTerms agg = (StringTerms) translator.toBucketAggregation(
            minThree,
            List.of(bucket("BrandA", 3), bucket("BrandB", 2), bucket("BrandC", 1))
        );

        assertEquals(1, agg.getBuckets().size());
        assertEquals("BrandA", agg.getBuckets().get(0).getKeyAsString());
        // min_doc_count exclusions do NOT feed sum_other_doc_count — InternalTerms.reduce only accumulates
        // what its size-bounded queue overflows, so mirroring it keeps DSL and legacy responses equal.
        assertEquals(0L, agg.getSumOfOtherDocCounts());
    }

    public void testToBucketAggregationAppliesMinDocCountBeforeSize() {
        TermsAggregationBuilder agg = new TermsAggregationBuilder("by_brand").field("brand").size(2).minDocCount(2);
        StringTerms terms = (StringTerms) translator.toBucketAggregation(
            agg,
            List.of(bucket("BrandA", 9), bucket("BrandB", 1), bucket("BrandC", 5), bucket("BrandD", 4))
        );

        // BrandB is filtered out entirely, so the size-2 page is the top two of what remains.
        assertEquals(
            List.of("BrandA", "BrandC"),
            List.of(terms.getBuckets().get(0).getKeyAsString(), terms.getBuckets().get(1).getKeyAsString())
        );
        assertEquals(4L, terms.getSumOfOtherDocCounts());
    }

    public void testToBucketAggregationTruncatesLongAndDoubleKeysToo() {
        TermsAggregationBuilder longAgg = new TermsAggregationBuilder("by_price").field("price").size(1).order(BucketOrder.key(true));
        LongTerms longTerms = (LongTerms) translator.toBucketAggregation(longAgg, List.of(bucket(700, 1), bucket(999L, 4)));
        assertEquals(1, longTerms.getBuckets().size());
        assertEquals(700L, longTerms.getBuckets().get(0).getKeyAsNumber().longValue());
        assertEquals(4L, longTerms.getSumOfOtherDocCounts());

        TermsAggregationBuilder doubleAgg = new TermsAggregationBuilder("by_rating").field("rating").size(1).order(BucketOrder.key(true));
        DoubleTerms doubleTerms = (DoubleTerms) translator.toBucketAggregation(doubleAgg, List.of(bucket(4.2, 1), bucket(4.5, 2)));
        assertEquals(1, doubleTerms.getBuckets().size());
        assertEquals(4.2, doubleTerms.getBuckets().get(0).getKeyAsNumber().doubleValue(), 0.0);
        assertEquals(2L, doubleTerms.getSumOfOtherDocCounts());
    }

    public void testToBucketAggregationRejectsMultiKeyEntry() {
        expectThrows(
            IllegalStateException.class,
            () -> translator.toBucketAggregation(brandAgg, List.of(new BucketEntry(List.of("BrandA", "extra"), 1L, null)))
        );
    }

    private static BucketEntry bucket(Object key, long docCount) {
        return new BucketEntry(List.of(key), docCount, InternalAggregations.EMPTY);
    }
}
