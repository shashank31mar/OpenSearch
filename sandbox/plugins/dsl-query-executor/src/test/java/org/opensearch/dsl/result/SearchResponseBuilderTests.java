/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.search.SearchModule;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchResponseBuilderTests extends OpenSearchTestCase {

    public void testBuildReturnsEmptyResponse() {
        SearchResponse response = SearchResponseBuilder.build(List.of(), new SearchSourceBuilder(), 42L * 1_000_000);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(42L, response.getTook().millis());
    }

    public void testBuildWithNullSearchSourceReturnsHitsOnly() {
        // TransportDslExecuteAction guards nothing: request.source() can be null, because a search with no
        // body is legal. That must be a hits-only 200, not an NPE.
        SearchResponse response = SearchResponseBuilder.build(List.of(), null, 42L * 1_000_000);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(0, response.getHits().getHits().length);
        assertNull(response.getAggregations());
    }

    public void testBuildWithNoAggregationsIsUnchanged() {
        // A body that asks for no aggregations must not grow an empty aggregations section.
        SearchResponse response = SearchResponseBuilder.build(List.of(), new SearchSourceBuilder().size(0), 42L * 1_000_000);

        assertEquals(200, response.status().getStatus());
        assertEquals(42L, response.getTook().millis());
        assertNull(response.getAggregations());
    }

    public void testDefaultBodyIsNotAnsweredWithEveryRow() {
        // The end-to-end shape of the size bug: a body with no `size` gets no fetch push-down, so the engine
        // hands back every matching row and the response used to carry all of them.
        SearchResponse response = SearchResponseBuilder.build(List.of(hitsResult(15)), new SearchSourceBuilder(), 42L * 1_000_000);

        assertEquals(SearchService.DEFAULT_SIZE, response.getHits().getHits().length);
        assertEquals(15L, response.getHits().getTotalHits().value());
    }

    // ---- Golden file driven SearchResponse generation tests ----

    /**
     * Auto-discovers all golden JSON files and validates that mock execution
     * rows produce the expected SearchResponse JSON via SearchResponseBuilder.build().
     */
    public void testGoldenFileSearchResponseGeneration() throws Exception {
        URL goldenDir = getClass().getClassLoader().getResource("golden");
        assertNotNull("Golden file resource directory not found", goldenDir);

        List<Path> goldenFiles;
        try (var stream = Files.list(Path.of(goldenDir.toURI()))) {
            goldenFiles = stream.filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList());
        }
        assertFalse("No golden files found", goldenFiles.isEmpty());

        List<String> failures = new ArrayList<>();
        for (Path file : goldenFiles) {
            String fileName = file.getFileName().toString();
            try {
                GoldenTestCase tc = GoldenFileLoader.load(fileName);
                CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

                // Build QueryPlan via forward path (needed to construct ExecutionResult)
                SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
                SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
                QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

                QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
                List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
                if (matchingPlans.isEmpty()) {
                    failures.add(fileName + ": No " + expectedType + " plan produced");
                    continue;
                }

                // One row set per emitted plan. A nested or sibling shape emits several plans, and the
                // assembler needs ALL of them: feeding only plan 0 would be a missing-granularity error.
                // mockResultRowsPerPlan is aligned with plans.get(type), which is the walker's declaration
                // order; the single-plan shapes keep using mockResultRows.
                List<List<List<Object>>> rowsPerPlan = tc.getMockResultRowsPerPlan() != null
                    ? tc.getMockResultRowsPerPlan()
                    : List.of(tc.getMockResultRows());
                if (rowsPerPlan.size() != matchingPlans.size()) {
                    failures.add(
                        fileName + ": " + matchingPlans.size() + " " + expectedType + " plans but " + rowsPerPlan.size() + " row sets"
                    );
                    continue;
                }

                List<ExecutionResult> results = new ArrayList<>();
                for (int i = 0; i < matchingPlans.size(); i++) {
                    List<Object[]> rows = new ArrayList<>();
                    for (List<Object> row : rowsPerPlan.get(i)) {
                        rows.add(row.toArray());
                    }
                    results.add(new ExecutionResult(matchingPlans.get(i), rows));
                }

                // Build and serialize SearchResponse
                SearchResponse response = SearchResponseBuilder.build(results, searchSource, 0L);
                String responseJson = Strings.toString(MediaTypeRegistry.JSON, response);

                Map<String, Object> actualOutput = XContentHelper.convertToMap(JsonXContent.jsonXContent, responseJson, false);

                // Deep copy expected to avoid mutating GoldenTestCase
                String expectedJson;
                try (var builder = JsonXContent.contentBuilder()) {
                    builder.map(tc.getExpectedOutputDsl());
                    expectedJson = builder.toString();
                }
                Map<String, Object> expectedOutput = XContentHelper.convertToMap(JsonXContent.jsonXContent, expectedJson, false);

                stripNonDeterministicFields(actualOutput);
                stripNonDeterministicFields(expectedOutput);

                if ("AGGREGATION".equals(tc.getPlanType())) {
                    normalizeAggregationBuckets(actualOutput);
                    normalizeAggregationBuckets(expectedOutput);
                }

                if (!expectedOutput.equals(actualOutput)) {
                    String expectedPretty, actualPretty;
                    try (var b = JsonXContent.contentBuilder().prettyPrint()) {
                        b.map(expectedOutput);
                        expectedPretty = b.toString();
                    }
                    try (var b = JsonXContent.contentBuilder().prettyPrint()) {
                        b.map(actualOutput);
                        actualPretty = b.toString();
                    }
                    failures.add(fileName + ": SearchResponse mismatch\n  Expected: " + expectedPretty + "\n  Actual:   " + actualPretty);
                }
            } catch (Exception e) {
                failures.add(fileName + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            fail("Golden file SearchResponse generation failures:\n" + String.join("\n", failures));
        }
    }

    /**
     * The corpus has to keep covering the two shapes that assembly can get wrong in ways a single-level
     * case cannot show: the parent/child join and the same-field sibling split. Deleting either file would
     * otherwise leave {@link #testGoldenFileSearchResponseGeneration} green while losing the coverage.
     */
    public void testGoldenCorpusCoversNestedAndSiblingShapes() throws Exception {
        URL goldenDir = getClass().getClassLoader().getResource("golden");
        assertNotNull("Golden file resource directory not found", goldenDir);

        List<String> fileNames;
        try (var stream = Files.list(Path.of(goldenDir.toURI()))) {
            fileNames = stream.map(p -> p.getFileName().toString()).collect(Collectors.toList());
        }

        assertTrue(
            "the 2-level nest shape must stay in the corpus: " + fileNames,
            fileNames.contains("nested_terms_with_avg_aggregation.json")
        );
        assertTrue(
            "the same-field sibling shape must stay in the corpus: " + fileNames,
            fileNames.contains("sibling_terms_same_field_aggregation.json")
        );
    }

    // ---- Helpers ----

    /** A HITS result of {@code rowCount} single-column rows. */
    private static ExecutionResult hitsResult(int rowCount) {
        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(
            QueryPlans.Type.HITS,
            TestUtils.createRelNodeWithColumns(List.of("name")),
            GranularityKeys.ROOT
        );
        List<Object[]> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(new Object[] { "doc-" + i });
        }
        return new ExecutionResult(plan, rows);
    }

    private SearchSourceBuilder parseSearchSource(Map<String, Object> inputDsl) throws IOException {
        String json;
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(inputDsl);
            json = builder.toString();
        }
        NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents()
        );
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(registry, DeprecationHandler.IGNORE_DEPRECATIONS, json)) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    @SuppressWarnings("unchecked")
    private void stripNonDeterministicFields(Map<String, Object> responseMap) {
        responseMap.remove("took");
        responseMap.remove("timed_out");
        responseMap.remove("_shards");
    }

    @SuppressWarnings("unchecked")
    private void normalizeAggregationBuckets(Map<String, Object> map) {
        Object aggs = map.get("aggregations");
        if (aggs instanceof Map) {
            normalizeBucketsRecursive((Map<String, Object>) aggs);
        }
    }

    /** Recursively sorts aggregation bucket lists by key for order-insensitive comparison. */
    @SuppressWarnings("unchecked")
    private void normalizeBucketsRecursive(Map<String, Object> aggMap) {
        for (Map.Entry<String, Object> entry : aggMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                Map<String, Object> aggBody = (Map<String, Object>) value;
                Object buckets = aggBody.get("buckets");
                if (buckets instanceof List) {
                    List<Map<String, Object>> bucketList = (List<Map<String, Object>>) buckets;
                    bucketList.sort(Comparator.comparing(b -> String.valueOf(b.get("key"))));
                    for (Map<String, Object> bucket : bucketList) {
                        for (Map.Entry<String, Object> bucketEntry : bucket.entrySet()) {
                            if (bucketEntry.getValue() instanceof Map) {
                                Map<String, Object> subAgg = (Map<String, Object>) bucketEntry.getValue();
                                if (subAgg.containsKey("buckets")) {
                                    normalizeBucketsRecursive(Map.of(bucketEntry.getKey(), subAgg));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
