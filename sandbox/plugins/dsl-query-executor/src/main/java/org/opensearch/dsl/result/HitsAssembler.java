/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchException;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Assembles the {@code hits} section of a {@link org.opensearch.action.search.SearchResponse} from the
 * rows of the {@link QueryPlans.Type#HITS} plan.
 *
 * <p>Column mapping is free and exact: {@link ExecutionResult#getFieldNames()} is the plan's row type in
 * the same order the rows are materialised, so zipping names against each {@code Object[]} reproduces the
 * projected document. No plan stringification is involved — see {@link AggregationAssembler} for why that
 * matters on the completion thread.
 *
 * <p><b>Known gap — an assembled hit carries no {@code _id} and no {@code _index}.</b> Neither is
 * reachable from here and neither is invented: the Calcite row type the engine returns is built from the
 * index's {@code MappingMetadata} document fields only
 * ({@code OpenSearchSchemaBuilder} in {@code analytics-api}), so there is no {@code _id} column to
 * project, and the DSL path acquires no OpenSearch searcher that could supply one.
 * {@link SearchHit#shard} is the only way to set {@code _index}, and calling it would mean fabricating a
 * node id and shard id — the same fabrication this class refuses for {@code hits.total} below. A client
 * that follows a search with a get/update/delete by id therefore cannot use these hits; that is a named
 * blocker on {@code DslQuerySettings.DSL_QUERY_ENABLED}, whose default stays {@code false}.
 */
public final class HitsAssembler {

    private HitsAssembler() {}

    /**
     * Assembles the hits section.
     *
     * <p>A request with {@code size == 0} emits no HITS plan at all, so no HITS result is a normal
     * outcome, not an error: it yields empty hits.
     *
     * @param results all execution results for the request; AGGREGATION results are ignored here
     * @param searchSource the request body the results came from; may be null — a search with no body is
     *     legal and takes the default page size
     * @return the assembled hits
     */
    public static SearchHits assemble(List<ExecutionResult> results, SearchSourceBuilder searchSource) {
        Objects.requireNonNull(results, "results must not be null");

        ExecutionResult hitsResult = null;
        for (ExecutionResult result : results) {
            if (result.getType() == QueryPlans.Type.HITS) {
                if (hitsResult != null) {
                    throw new IllegalStateException("A request produces at most one HITS plan, but two HITS results arrived");
                }
                hitsResult = result;
            }
        }
        if (hitsResult == null) {
            return SearchHits.empty(true);
        }

        List<String> fieldNames = hitsResult.getFieldNames();
        // The request's `size` is NOT pushed down when pagination is at its defaults:
        // SortConverter.buildFetch returns no fetch literal unless `hasNonDefaultPagination`, so a plain
        // `POST /idx/_search` (and an explicit `size: 10, from: 0`) produces a plan with no LIMIT and the
        // engine streams EVERY matching row. Clamping here is what keeps a default search from answering
        // with the whole index — and from building that many SearchHits, each with its own JSON source, on
        // the engine's completion thread. When the fetch WAS pushed down the engine already returned at
        // most `size` rows, so the clamp is then a no-op. `from` is deliberately NOT re-applied: any
        // from > 0 makes SortConverter applicable and emits the offset, so the engine has already skipped.
        int limit = requestedSize(searchSource);
        List<SearchHit> hits = new ArrayList<>();
        long rowCount = 0;
        for (Object[] row : hitsResult.getRows()) {
            rowCount++;
            // Rows past the page are still counted (cheap) but never materialised into a hit: the count is
            // the only honest lower bound available for hits.total.
            if (hits.size() < limit) {
                hits.add(toSearchHit(hits.size(), fieldNames, row));
            }
        }

        // GREATER_THAN_OR_EQUAL_TO, not EQUAL_TO: the returned row count is a lower bound on the matching
        // document count, because the engine truncates its row stream and the request's `size` is not
        // pushed down, so "this many at least" is all that is knowable here. The real count needs
        // ExecutionMetadata from the engine — M-F4 (SC-11) — which does not exist yet. A fabricated
        // EQUAL_TO would be a wrong number that every caller would believe.
        TotalHits total = new TotalHits(rowCount, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        // NaN renders as `"max_score": null`, which is what a non-scoring path honestly has.
        return new SearchHits(hits.toArray(new SearchHit[0]), total, Float.NaN);
    }

    /**
     * The number of hits the request asked for. {@code -1} is {@code SearchSourceBuilder}'s "unset"
     * sentinel, and a null body is a legal search, so both take {@code SearchService.DEFAULT_SIZE} — the
     * same resolution {@code SortConverter} does, so the clamp can never disagree with the pushed-down
     * fetch.
     */
    private static int requestedSize(SearchSourceBuilder searchSource) {
        if (searchSource == null || searchSource.size() == -1) {
            return SearchService.DEFAULT_SIZE;
        }
        return searchSource.size();
    }

    private static SearchHit toSearchHit(int docId, List<String> fieldNames, Object[] row) {
        if (row.length != fieldNames.size()) {
            throw new IllegalStateException(
                "HITS row has " + row.length + " columns but the plan's row type declares " + fieldNames.size()
            );
        }
        // LinkedHashMap, and null values kept: a projected field that is absent from the document is a
        // null column, and dropping it would make the source disagree with the projection.
        Map<String, Object> source = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            source.put(fieldNames.get(i), row[i]);
        }

        SearchHit hit = new SearchHit(docId);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(source);
            hit.sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            // In-memory serialisation: an IOException here is a real failure, and it must surface as one
            // rather than as a hit with an empty _source.
            throw new OpenSearchException("Failed to serialise DSL hit source", e);
        }
        return hit;
    }
}
