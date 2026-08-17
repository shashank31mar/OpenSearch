/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for DSL aggregation conversion.
 * Uses matchAllQuery; focus is on aggregation plan building.
 */
@AwaitsFix(bugUrl = "this source set cannot start a node: AnalyticsPlugin.createComponents:168 throws"
    + " \"ArrowNativeAllocator not available; arrow-base plugin must be installed\" because"
    + " DslIntegTestBase.nodePlugins installs neither arrow-base nor arrow-flight-rpc, and even"
    + " with those it has no execution backend (see build.gradle: the internalClusterTest block"
    + " is analytics-engine-coordinator's minus the DataFusion native library). Response"
    + " assembly is no longer the blocker; the test HOST is. Un-disabling needs this host"
    + " brought up to sandbox/qa/analytics-engine-coordinator's plugin set + feature flags, or"
    + " these cases moved to the REST host that installs real plugin zips.")
public class DslAggregationIT extends DslIntegTestBase {

    public void testMetricOnly() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("avg_price").field("price"))));
    }

    public void testMultipleMetrics() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(AggregationBuilders.avg("avg_price").field("price"))
                    .aggregation(AggregationBuilders.sum("total_price").field("price"))
                    .aggregation(AggregationBuilders.min("min_price").field("price"))
                    .aggregation(AggregationBuilders.max("max_price").field("price"))
            )
        );
    }

    public void testTermsBucket() {
        createTestIndex();
        assertBucketCount(
            search(new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"))),
            "by_brand",
            1
        );
    }

    public void testTermsBucketWithMetric() {
        createTestIndex();
        assertBucketCount(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_brand").field("brand")
                            .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                    )
            ),
            "by_brand",
            1
        );
    }

    public void testNestedBuckets() {
        createTestIndex();
        assertBucketCount(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_brand").field("brand")
                            .subAggregation(AggregationBuilders.sum("total").field("price"))
                            .subAggregation(
                                new TermsAggregationBuilder("by_name").field("name")
                                    .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                            )
                    )
            ),
            "by_brand",
            1
        );
    }

    public void testAggsWithHits() {
        createTestIndex();
        // size > 0 with aggs produces both HITS + AGGREGATION plans
        assertHasHits(search(new SearchSourceBuilder().size(10).aggregation(AggregationBuilders.avg("avg_price").field("price"))));
    }

    public void testTermsBucketOrderByKeyAsc() {
        createTestIndex();
        assertBucketCount(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(true)))
            ),
            "by_brand",
            1
        );
    }

    public void testTermsBucketOrderByKeyDesc() {
        createTestIndex();
        assertBucketCount(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(false)))
            ),
            "by_brand",
            1
        );
    }

    public void testTermsBucketOrderByCountAsc() {
        createTestIndex();
        assertBucketCount(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(true)))
            ),
            "by_brand",
            1
        );
    }

    public void testTermsBucketOrderByMetric() {
        createTestIndex();
        assertBucketCount(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_brand").field("brand")
                            .order(BucketOrder.aggregation("avg_price", false))
                            .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                    )
            ),
            "by_brand",
            1
        );
    }
}
