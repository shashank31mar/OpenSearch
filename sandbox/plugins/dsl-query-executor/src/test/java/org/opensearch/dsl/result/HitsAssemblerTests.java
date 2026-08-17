/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HitsAssemblerTests extends OpenSearchTestCase {

    public void testRowsBecomeHitsWithFieldNamesAsSourceKeys() {
        SearchHits hits = HitsAssembler.assemble(
            List.of(
                hitsResult(List.of("name", "price", "brand", "rating"), row("laptop", 999, "BrandA", 4.5), row("phone", 699, "BrandB", 4.2))
            ),
            new SearchSourceBuilder()
        );

        assertEquals(2, hits.getHits().length);
        Map<String, Object> first = hits.getHits()[0].getSourceAsMap();
        assertEquals("laptop", first.get("name"));
        assertEquals(999, first.get("price"));
        assertEquals("BrandA", first.get("brand"));
        assertEquals(4.5, (Double) first.get("rating"), 0.0);
        assertEquals("phone", hits.getHits()[1].getSourceAsMap().get("name"));
    }

    public void testSizeZeroYieldsEmptyHits() {
        // size == 0 emits no HITS plan at all, so an aggregation-only result set is normal, not an error.
        SearchHits hits = HitsAssembler.assemble(List.of(aggResult()), new SearchSourceBuilder().size(0));

        assertEquals(0, hits.getHits().length);
        assertNotNull(hits.getTotalHits());
        assertEquals(0L, hits.getTotalHits().value());
    }

    public void testNoResultsAtAllYieldsEmptyHits() {
        assertEquals(0, HitsAssembler.assemble(List.of(), null).getHits().length);
    }

    public void testNullColumnValueIsPreservedInSource() {
        // A projected field absent from the document is a null column. Dropping it would make the source
        // disagree with the projection the caller asked for.
        SearchHits hits = HitsAssembler.assemble(
            List.of(hitsResult(List.of("name", "price"), row("laptop", null))),
            new SearchSourceBuilder()
        );

        Map<String, Object> source = hits.getHits()[0].getSourceAsMap();
        assertTrue("the projected column must be present", source.containsKey("price"));
        assertNull(source.get("price"));
    }

    public void testTotalHitsIsALowerBoundNotAFabricatedCount() {
        // The row count is all that is knowable without engine metadata (M-F4 / SC-11): the relation must
        // say "at least", never claim equality it cannot support.
        SearchHits hits = HitsAssembler.assemble(
            List.of(hitsResult(List.of("name"), row("laptop"), row("phone"))),
            new SearchSourceBuilder()
        );

        assertEquals(2L, hits.getTotalHits().value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, hits.getTotalHits().relation());
    }

    public void testDefaultRequestReturnsAtMostTheDefaultPageSize() {
        // A body with no `size` is the default page of 10, and SortConverter pushes NO fetch literal for
        // default pagination, so the engine hands over every matching row. Without a clamp here a plain
        // `POST /idx/_search` over a 5k-document index answered with 5k hits.
        SearchHits hits = HitsAssembler.assemble(List.of(hitsResult(List.of("name"), rows(15))), new SearchSourceBuilder());

        assertEquals(SearchService.DEFAULT_SIZE, hits.getHits().length);
        assertEquals("doc-0", hits.getHits()[0].getSourceAsMap().get("name"));
        assertEquals("doc-9", hits.getHits()[SearchService.DEFAULT_SIZE - 1].getSourceAsMap().get("name"));
        // The page shrank; the lower bound on matching documents did not.
        assertEquals(15L, hits.getTotalHits().value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, hits.getTotalHits().relation());
    }

    public void testExplicitDefaultSizeIsAlsoClamped() {
        // `size: 10, from: 0` is byte-identical to the default as far as SortConverter is concerned
        // (hasNonDefaultPagination is false), so it gets no LIMIT either.
        SearchHits hits = HitsAssembler.assemble(
            List.of(hitsResult(List.of("name"), rows(15))),
            new SearchSourceBuilder().from(0).size(10)
        );

        assertEquals(10, hits.getHits().length);
    }

    public void testNullSearchSourceTakesTheDefaultPageSize() {
        // A search with no body at all is legal and must not answer with the whole index either.
        SearchHits hits = HitsAssembler.assemble(List.of(hitsResult(List.of("name"), rows(15))), null);

        assertEquals(SearchService.DEFAULT_SIZE, hits.getHits().length);
    }

    public void testRequestedSizeSmallerThanTheRowsWins() {
        SearchHits hits = HitsAssembler.assemble(List.of(hitsResult(List.of("name"), rows(15))), new SearchSourceBuilder().size(3));

        assertEquals(3, hits.getHits().length);
        assertEquals("doc-2", hits.getHits()[2].getSourceAsMap().get("name"));
    }

    public void testFewerRowsThanTheRequestedSizeReturnsThemAll() {
        SearchHits hits = HitsAssembler.assemble(List.of(hitsResult(List.of("name"), rows(4))), new SearchSourceBuilder().size(50));

        assertEquals(4, hits.getHits().length);
    }

    public void testRowWithWrongColumnCountFails() {
        ExecutionResult broken = hitsResult(List.of("name", "price"), row("laptop"));

        expectThrows(IllegalStateException.class, () -> HitsAssembler.assemble(List.of(broken), new SearchSourceBuilder()));
    }

    // ---- Helpers ----

    private static Object[] row(Object... values) {
        return values;
    }

    /** {@code n} single-column rows, named so a truncation can be told from a reordering. */
    private static Object[][] rows(int n) {
        Object[][] rows = new Object[n][];
        for (int i = 0; i < n; i++) {
            rows[i] = row("doc-" + i);
        }
        return rows;
    }

    private static ExecutionResult hitsResult(List<String> columnNames, Object[]... rows) {
        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(
            QueryPlans.Type.HITS,
            TestUtils.createRelNodeWithColumns(columnNames),
            GranularityKeys.ROOT
        );
        return new ExecutionResult(plan, Arrays.asList(rows));
    }

    private static ExecutionResult aggResult() {
        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(
            QueryPlans.Type.AGGREGATION,
            TestUtils.createRelNodeWithColumns(List.of("brand", "_count")),
            "0:1#a:7#5#brand"
        );
        return new ExecutionResult(plan, List.<Object[]>of(new Object[] { "BrandA", 3L }));
    }
}
