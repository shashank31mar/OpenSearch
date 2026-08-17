/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

/**
 * Base class for DSL query executor integration tests.
 * Provides shared index setup and search helper.
 *
 * <p><b>Every subclass is class-level {@code @AwaitsFix}, and response assembly is no longer why.</b>
 * {@code SearchResponseBuilder} now renders real hits and real aggregations, so the content assertions
 * these ITs are missing are finally satisfiable — but this source set cannot start a node to run them
 * against: {@code AnalyticsPlugin.createComponents} throws {@code ArrowNativeAllocator not available}
 * because {@link #nodePlugins()} installs neither {@code arrow-base} nor {@code arrow-flight-rpc}, and
 * this module's {@code build.gradle} states it has no execution backend either. Compare
 * {@code sandbox/qa/analytics-engine-coordinator}'s ITs, which install six plugins plus the
 * {@code PLUGGABLE_DATAFORMAT} / {@code STREAM_TRANSPORT} feature flags. Bringing this host to that
 * level is its own change; the DSL path's end-to-end correctness evidence lives in the REST host, which
 * installs real plugin zips.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public abstract class DslIntegTestBase extends OpenSearchIntegTestCase {

    protected static final String INDEX = "test-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class, DslQueryExecutorPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // MANDATORY, and the whole point of these ITs: dsl.query.enabled defaults to FALSE, and
        // SearchActionFilter's gate is `if (dslEnabled && ...)`, so a subclass that says nothing about the
        // setting starts a cluster on which every search() takes chain.proceed — the legacy Lucene path.
        // Every assertion below would then pass while testing none of the DSL code, which is strictly
        // worse than the @AwaitsFix state it replaces: a red or skipped test is visible, a green-but-wrong
        // one is not. A static node setting rather than a dynamic update because internalClusterTest has
        // no _cluster/settings REST PUT, and a static setting is applied at node start so it also covers
        // the @Before / createTestIndex phase. SC-2 is NodeScope + Dynamic, which makes this legal.
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("dsl.query.enabled", true).build();
    }

    protected void createTestIndex() {
        createIndex(INDEX);
        ensureGreen();
        client().prepareIndex(INDEX)
            .setSource("{\"name\":\"laptop\",\"price\":1200,\"brand\":\"brandX\",\"rating\":4.5}", XContentType.JSON)
            .get();
        refresh(INDEX);
    }

    protected SearchResponse search(SearchSourceBuilder source) {
        return client().search(new SearchRequest(INDEX).source(source)).actionGet();
    }

    /**
     * Asserts the transport-level outcome only. On its own this is vacuous: the shard counts are
     * hardcoded to {@code 0, 0, 0}, so {@code status()} is an unconditional {@code 200} whatever the
     * response contains. A caller that wants a real assertion pairs it with a content check.
     */
    protected void assertOk(SearchResponse response) {
        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        // M-F4 (SC-11): getSuccessfulShards() > 0 and getFailedShards() == 0 belong here, and are
        // unsatisfiable until the engine reports real shard counts — successful is hardcoded 0.
    }

    /**
     * Asserts a hits query returned the document {@link #createTestIndex()} indexed. Unlike
     * {@link #assertOk} this cannot pass on an empty response, which is what made these ITs vacuous.
     *
     * @param response the search response
     */
    protected void assertHasHits(SearchResponse response) {
        assertOk(response);
        assertTrue("expected non-empty hits for a hits query", response.getHits().getHits().length > 0);
    }

    /**
     * Asserts an aggregation query returned the named aggregation with the expected bucket count.
     *
     * @param response the search response
     * @param aggName the requested aggregation's name
     * @param expectedBuckets the number of buckets the one indexed document implies
     */
    protected void assertBucketCount(SearchResponse response, String aggName, int expectedBuckets) {
        assertOk(response);
        assertNotNull("expected an aggregations section", response.getAggregations());
        MultiBucketsAggregation agg = response.getAggregations().get(aggName);
        assertNotNull("expected aggregation [" + aggName + "]", agg);
        assertEquals(expectedBuckets, agg.getBuckets().size());
    }
}
