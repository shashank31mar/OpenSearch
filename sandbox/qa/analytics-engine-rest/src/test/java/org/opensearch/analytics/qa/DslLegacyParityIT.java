/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * DSL aggregation output equals legacy aggregation output, <b>sequentially</b> — at the shipped default
 * {@code max_parallel_sub_plans = 1}. This is the feature's first real correctness evidence: until
 * response assembly landed, every DSL response was an empty {@code 200}, so no test anywhere could compare
 * output at all.
 *
 * <p>The control variable here is the <em>path</em>, not the width. Fan-out parity is
 * {@link DslFanOutParityIT}'s job, at {@code = 2}, and the two must stay separate: one IT changing two
 * variables cannot say which one broke.
 *
 * <p>Own {@code integTestDslParity} task and cluster, and excluded from the default {@code integTest}, for
 * the standard reason: {@code dsl.query.enabled} is cluster-wide, and the default task runs ~150 sibling
 * ITs against one shared 2-node cluster with a single fork. It is restored to its <b>default</b> (cleared,
 * i.e. {@code false}) in {@link #restoreSettings()} — never to {@code true}, which would leave every later
 * IT on the DSL path.
 *
 * <p>See {@link DslParityTestBase} for why the baseline comes from a Lucene twin rather than the same
 * index with the setting flipped, why only the {@code aggregations} section is compared, and why the corpus
 * is deliberately small.
 */
public class DslLegacyParityIT extends DslParityTestBase {

    @After
    public void restoreSettings() throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "null");
        putTransient(MAX_PARALLEL_SUB_PLANS_KEY, "null");
    }

    /**
     * {@code terms(brand) > terms(category) > avg(price)}: the DSL path's aggregations must equal legacy's.
     * This one assertion covers the granularity key, the map-keyed join, the terms bucket leaf and the AVG
     * metric leaf at once — any of them wrong and the two sections differ.
     *
     * <p>{@code _shards.successful > 0}, {@code _shards.failed == 0} and {@code took > 0} are NOT asserted:
     * the DSL path hardcodes its shard counts and reports conversion time only, so those assertions land
     * with the engine-metadata work (M-F4 / SC-11), not here. Asserting them now would be red for a reason
     * unrelated to assembly.
     */
    public void testNestedTermsAvgMatchesLegacyAggregation() throws IOException {
        Map<String, Object> legacy = legacyAggregations(nestedAggBody());

        putTransient(DSL_QUERY_ENABLED_KEY, "true");
        Map<String, Object> dsl = aggregationsOf(DSL_INDEX, nestedAggBody());

        assertEquals("the DSL path must return legacy's nested aggregation", legacy, dsl);
    }

    /**
     * The same-field sibling shape. Before the aggregation name entered the granularity key both siblings
     * hashed to one key, so the walker merged their metrics into a single plan and one of the two
     * aggregations could not be produced at all. Legacy returns two independent aggregations; so must the
     * DSL path.
     */
    public void testSiblingTermsOnSameFieldMatchLegacy() throws IOException {
        Map<String, Object> legacy = legacyAggregations(siblingAggBody());
        assertEquals("the reference itself must carry both siblings: " + legacy, 2, legacy.size());

        putTransient(DSL_QUERY_ENABLED_KEY, "true");
        Map<String, Object> dsl = aggregationsOf(DSL_INDEX, siblingAggBody());

        assertEquals("two aggregations over one field must stay separate", legacy, dsl);
    }

    /**
     * The parent/child join, asserted directly rather than only through whole-section equality, so a
     * failure names the defect instead of printing two large maps. Assembly is map-keyed on the plan's
     * granularity, and a mis-join surfaces as a child bucket under the wrong parent — which is exactly what
     * a per-parent check catches.
     */
    public void testTwoLevelNestJoinsChildBucketsUnderCorrectParent() throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "true");
        Map<String, Object> dsl = aggregationsOf(DSL_INDEX, nestedAggBody());

        // The fixture: brand-a is phone x2 + laptop x1, brand-b is phone x1 + laptop x2,
        // brand-c is phone x1 + laptop x2. Averages are exact by construction.
        assertBucket(dsl, "brand-a", 3, Map.of("phone", 200.0, "laptop", 500.0), Map.of("phone", 2L, "laptop", 1L));
        assertBucket(dsl, "brand-b", 3, Map.of("phone", 200.0, "laptop", 600.0), Map.of("phone", 1L, "laptop", 2L));
        assertBucket(dsl, "brand-c", 3, Map.of("phone", 50.0, "laptop", 200.0), Map.of("phone", 1L, "laptop", 2L));
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /**
     * The legacy reference: the Lucene twin, with the DSL path explicitly off so a leaked {@code true} from
     * a sibling test cannot silently turn the reference into a second DSL run.
     */
    private Map<String, Object> legacyAggregations(String body) throws IOException {
        putTransient(DSL_QUERY_ENABLED_KEY, "false");
        return aggregationsOf(LEGACY_INDEX, body);
    }

    @SuppressWarnings("unchecked")
    private static void assertBucket(
        Map<String, Object> aggregations,
        String brand,
        int expectedDocCount,
        Map<String, Double> expectedAveragesByCategory,
        Map<String, Long> expectedDocCountsByCategory
    ) {
        Map<String, Object> byBrand = (Map<String, Object>) aggregations.get("by_brand");
        assertNotNull("no by_brand aggregation in " + aggregations, byBrand);
        List<Map<String, Object>> brandBuckets = (List<Map<String, Object>>) byBrand.get("buckets");

        Map<String, Object> bucket = brandBuckets.stream()
            .filter(candidate -> brand.equals(candidate.get("key")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no bucket [" + brand + "] in " + brandBuckets));
        assertEquals("doc_count for " + brand, expectedDocCount, ((Number) bucket.get("doc_count")).intValue());

        Map<String, Object> byCategory = (Map<String, Object>) bucket.get("by_category");
        assertNotNull("no by_category under " + brand + ": " + bucket, byCategory);
        List<Map<String, Object>> categoryBuckets = (List<Map<String, Object>>) byCategory.get("buckets");
        assertEquals("child bucket count under " + brand, expectedAveragesByCategory.size(), categoryBuckets.size());

        for (Map<String, Object> categoryBucket : categoryBuckets) {
            String category = String.valueOf(categoryBucket.get("key"));
            Double expectedAverage = expectedAveragesByCategory.get(category);
            assertNotNull("unexpected child bucket [" + category + "] under " + brand, expectedAverage);
            assertEquals(
                "doc_count for " + brand + "/" + category,
                expectedDocCountsByCategory.get(category).longValue(),
                ((Number) categoryBucket.get("doc_count")).longValue()
            );
            Map<String, Object> avg = (Map<String, Object>) categoryBucket.get("avg_price");
            assertNotNull("no avg_price under " + brand + "/" + category, avg);
            assertEquals("avg_price for " + brand + "/" + category, expectedAverage, ((Number) avg.get("value")).doubleValue(), 0.0);
        }
    }
}
