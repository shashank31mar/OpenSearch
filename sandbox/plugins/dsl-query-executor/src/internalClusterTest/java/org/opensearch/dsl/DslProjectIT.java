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

/**
 * Integration tests for DSL _source filtering (projection) conversion.
 * Uses matchAllQuery; focus is on _source includes/excludes behavior.
 */
@AwaitsFix(bugUrl = "this source set cannot start a node: AnalyticsPlugin.createComponents:168 throws"
    + " \"ArrowNativeAllocator not available; arrow-base plugin must be installed\" because"
    + " DslIntegTestBase.nodePlugins installs neither arrow-base nor arrow-flight-rpc, and even"
    + " with those it has no execution backend (see build.gradle: the internalClusterTest block"
    + " is analytics-engine-coordinator's minus the DataFusion native library). Response"
    + " assembly is no longer the blocker; the test HOST is. Un-disabling needs this host"
    + " brought up to sandbox/qa/analytics-engine-coordinator's plugin set + feature flags, or"
    + " these cases moved to the REST host that installs real plugin zips.")
public class DslProjectIT extends DslIntegTestBase {

    public void testNoSourceFiltering() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder()));
    }

    public void testIncludeSpecificFields() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().fetchSource(new String[] { "name", "price" }, null)));
    }

    public void testExcludeFields() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().fetchSource(new String[] {}, new String[] { "rating" })));
    }

    public void testSourceDisabled() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().fetchSource(false)));
    }

    public void testWildcardIncludes() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().fetchSource(new String[] { "na*" }, null)));
    }

    public void testWildcardExcludes() {
        createTestIndex();
        assertHasHits(search(new SearchSourceBuilder().fetchSource(new String[] {}, new String[] { "ra*" })));
    }
}
