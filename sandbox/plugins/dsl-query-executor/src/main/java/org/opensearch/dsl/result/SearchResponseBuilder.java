/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * Builds a {@link SearchResponse} from execution results.
 */
public class SearchResponseBuilder {

    /**
     * One assembler for the life of the classloader, not one per response.
     * {@code AggregationRegistryFactory.create()} populates a {@code HashMap} of translators that is
     * never written again once the factory returns, and every translator is stateless, so the registry is
     * safe to share across concurrent SEARCH threads (publication is the class initialiser's). Building it
     * per response allocated the whole registry even for a request with no aggregations at all.
     */
    private static final AggregationAssembler AGGREGATION_ASSEMBLER = new AggregationAssembler(AggregationRegistryFactory.create());

    private SearchResponseBuilder() {}

    /**
     * Builds a SearchResponse from the given results and timing.
     *
     * <p>The original request body is threaded in because the response leaves need the
     * {@code AggregationBuilder} tree the caller sent — bucket name, requested order, thresholds — and
     * nothing on the execution path carries it: a {@code QueryPlan} is {@code (type, relNode,
     * granularity)} and an {@code ExecutionResult} is {@code (plan, rows)}. Rebuilding that tree from the
     * {@code RelNode} would be both lossy and unsafe on this thread.
     *
     * @param results execution results from the plan executor
     * @param searchSource the request body the results came from; may be null — a search with no body is
     *     legal and yields the hits-only response
     * @param convertTimeNanos time spent in DSL-to-RelNode conversion, in nanoseconds
     * @return a SearchResponse
     */
    // TODO: M-F4 (SC-11) — the analytics engine has no channel for execution metadata today, so these
    // stay unpopulated and are NOT invented here:
    // - executionTimeNanos: query execution time (took is conversion time only)
    // - totalDocCount: total matching documents for hits.total
    // - terminatedEarly: whether execution was terminated early
    // - timedOut: whether execution timed out
    // - shardInfo: total/successful/skipped/failed shard counts. These four ints ARE the HTTP status
    // (SearchResponse.status() = RestStatus.status(successful, total, failures)), so the hardcoded
    // zeros below make every response a 200. Once real counts exist, failedShards > 0 must produce
    // the honest non-200 rather than a 200 wrapping a partial failure.
    public static SearchResponse build(List<ExecutionResult> results, SearchSourceBuilder searchSource, long convertTimeNanos) {
        long tookInMillis = convertTimeNanos / 1_000_000;
        SearchHits hits = HitsAssembler.assemble(results, searchSource);
        InternalAggregations aggregations = AGGREGATION_ASSEMBLER.assemble(results, searchSource);
        SearchResponseSections sections = new SearchResponseSections(hits, aggregations, null, false, null, null, 0);
        return new SearchResponse(sections, null, 0, 0, 0, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
