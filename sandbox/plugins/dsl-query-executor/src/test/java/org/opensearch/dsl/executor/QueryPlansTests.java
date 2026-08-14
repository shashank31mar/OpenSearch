/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class QueryPlansTests extends OpenSearchTestCase {

    private RelNode relNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        relNode = TestUtils.createTestRelNode();
    }

    public void testBuilderCreatesSinglePlan() {
        QueryPlans plans = new QueryPlans.Builder().add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode, GranularityKeys.ROOT))
            .build();

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testBuilderCreatesMultiplePlans() {
        QueryPlans plans = new QueryPlans.Builder().add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode, GranularityKeys.ROOT))
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode, GranularityKeys.ROOT))
            .build();

        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        assertEquals(1, plans.get(QueryPlans.Type.HITS).size());
        assertEquals(1, plans.get(QueryPlans.Type.AGGREGATION).size());
    }

    public void testGetReturnsMultiplePlansOfSameType() {
        QueryPlans plans = new QueryPlans.Builder().add(
            new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode, GranularityKeys.ROOT)
        ).add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode, GranularityKeys.ROOT)).build();

        assertEquals(2, plans.get(QueryPlans.Type.AGGREGATION).size());
    }

    public void testBuilderAllowsEmpty() {
        // Empty plans are valid (e.g. size=0, no aggs — metadata-only response)
        QueryPlans plans = new QueryPlans.Builder().build();
        assertEquals(0, plans.getAll().size());
    }

    public void testGetReturnsEmptyForMissingType() {
        QueryPlans plans = new QueryPlans.Builder().add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode, GranularityKeys.ROOT))
            .build();

        assertTrue(plans.get(QueryPlans.Type.AGGREGATION).isEmpty());
    }

    public void testPlansAreImmutable() {
        QueryPlans plans = new QueryPlans.Builder().add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode, GranularityKeys.ROOT))
            .build();

        expectThrows(
            UnsupportedOperationException.class,
            () -> plans.getAll().add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode, GranularityKeys.ROOT))
        );
    }

    public void testQueryPlanRejectsNullArguments() {
        expectThrows(NullPointerException.class, () -> new QueryPlans.QueryPlan(QueryPlans.Type.HITS, null, GranularityKeys.ROOT));
        expectThrows(NullPointerException.class, () -> new QueryPlans.QueryPlan(null, relNode, GranularityKeys.ROOT));
    }

    public void testQueryPlanCarriesGranularity() {
        String granularity = GranularityKeys.granularityKey(List.of(new FieldGrouping("by_brand", List.of("brand"))));

        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode, granularity);

        assertEquals(granularity, plan.granularity());
        assertEquals(GranularityKeys.ROOT, new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode, GranularityKeys.ROOT).granularity());
    }

    public void testQueryPlanRejectsNullGranularity() {
        expectThrows(NullPointerException.class, () -> new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode, null));
    }
}
