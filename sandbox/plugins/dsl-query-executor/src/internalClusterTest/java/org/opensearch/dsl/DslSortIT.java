/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * Integration tests for DSL sort and pagination conversion.
 * Uses matchAllQuery; focus is on sort/from/size behavior.
 */
@AwaitsFix(bugUrl = "this source set cannot start a node: AnalyticsPlugin.createComponents:168 throws"
    + " \"ArrowNativeAllocator not available; arrow-base plugin must be installed\" because"
    + " DslIntegTestBase.nodePlugins installs neither arrow-base nor arrow-flight-rpc, and even"
    + " with those it has no execution backend (see build.gradle: the internalClusterTest block"
    + " is analytics-engine-coordinator's minus the DataFusion native library). Response"
    + " assembly is no longer the blocker; the test HOST is. Un-disabling needs this host"
    + " brought up to sandbox/qa/analytics-engine-coordinator's plugin set + feature flags, or"
    + " these cases moved to the REST host that installs real plugin zips.")
public class DslSortIT extends DslIntegTestBase {

    public void testDefaultPagination() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder()));
    }

    public void testSortAscending() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().sort("name", SortOrder.ASC)));
    }

    public void testSortDescending() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().sort("price", SortOrder.DESC)));
    }

    public void testMultipleSortFields() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().sort("brand", SortOrder.ASC).sort("price", SortOrder.DESC)));
    }

    public void testCustomSize() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().size(5)));
    }

    public void testFromAndSize() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().from(0).size(5)));
    }

    public void testFromOffset() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().from(10).size(5)));
    }
}
