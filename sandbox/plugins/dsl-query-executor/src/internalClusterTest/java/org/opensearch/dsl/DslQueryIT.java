/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for DSL query conversion (filter path).
 * Uses various query types; sort and projection use defaults.
 */
@AwaitsFix(bugUrl = "this source set cannot start a node: AnalyticsPlugin.createComponents:168 throws"
    + " \"ArrowNativeAllocator not available; arrow-base plugin must be installed\" because"
    + " DslIntegTestBase.nodePlugins installs neither arrow-base nor arrow-flight-rpc, and even"
    + " with those it has no execution backend (see build.gradle: the internalClusterTest block"
    + " is analytics-engine-coordinator's minus the DataFusion native library). Response"
    + " assembly is no longer the blocker; the test HOST is. Un-disabling needs this host"
    + " brought up to sandbox/qa/analytics-engine-coordinator's plugin set + feature flags, or"
    + " these cases moved to the REST host that installs real plugin zips.")
public class DslQueryIT extends DslIntegTestBase {

    public void testNoQuery() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder()));
    }

    public void testMatchAll() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));
    }

    public void testTermQuery() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "laptop"))));
    }

    public void testTermsQuery() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().query(QueryBuilders.termsQuery("name", "laptop", "phone"))));
    }

    public void testTermsQueryWithBoostThrowsException() {
        createTestIndex();
        expectThrows(
            ConversionException.class,
            () -> search(new SearchSourceBuilder().query(QueryBuilders.termsQuery("name", "laptop").boost(2.0f)))
        );
    }

    public void testTermsQueryWithNameThrowsException() {
        createTestIndex();
        expectThrows(
            ConversionException.class,
            () -> search(new SearchSourceBuilder().query(QueryBuilders.termsQuery("name", "laptop").queryName("my_query")))
        );
    }

    public void testTermsQueryWithValueTypeThrowsException() {
        createTestIndex();
        expectThrows(
            ConversionException.class,
            () -> search(
                new SearchSourceBuilder().query(
                    QueryBuilders.termsQuery("name", "laptop").valueType(org.opensearch.index.query.TermsQueryBuilder.ValueType.BITMAP)
                )
            )
        );
    }

    public void testWildcardQueryWithUnresolvedNode() {
        createTestIndex();
        // Wildcard query is not converted to standard Rex — wraps in UnresolvedQueryCall.
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "lap*"))));
    }

    public void testFailsForNonexistentIndex() {
        expectThrows(
            Exception.class,
            () -> client().search(new SearchRequest("nonexistent-index").source(new SearchSourceBuilder())).actionGet()
        );
    }

    public void testFailsForMultipleIndices() {
        createTestIndex();
        createIndex("test-index-2");
        ensureGreen();

        expectThrows(
            Exception.class,
            () -> client().search(new SearchRequest(INDEX, "test-index-2").source(new SearchSourceBuilder())).actionGet()
        );
    }

    public void testExistsQuery() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().query(QueryBuilders.existsQuery("name"))));
    }

    public void testExistsQueryWithBoostFails() {
        createTestIndex();
        expectThrows(Exception.class, () -> search(new SearchSourceBuilder().query(QueryBuilders.existsQuery("name").boost(2.0f))));
    }

    // TODO: Enable once BooleanQueryTranslatorExists is supported
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/21442")
    public void testExistsQueryWithBool() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().query(
                    QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("name")).filter(QueryBuilders.termQuery("brand", "brandX"))
                )
            )
        );
    }
}
