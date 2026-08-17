/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Shared fixture for the two DSL-vs-legacy aggregation parity ITs: {@link DslLegacyParityIT} (sequential,
 * at the shipped {@code max_parallel_sub_plans = 1}) and {@link DslFanOutParityIT} (at
 * {@code max_parallel_sub_plans = 2}). Extracted rather than copy-pasted so both compare the same
 * aggregations over the same documents — a divergence in either would make the two verdicts
 * incomparable.
 *
 * <p><b>Why a twin index and not one index with the setting flipped.</b> The obvious shape — search one
 * index with {@code dsl.query.enabled: false} for the baseline, then again with it {@code true} — does not
 * work here: the DSL path is only exercised over a parquet-primary index, and the legacy Lucene path
 * cannot serve one at all ({@code IndexShard.ensureLuceneSearchable} answers {@code 400}). So the baseline
 * comes from a plain-Lucene twin carrying byte-identical documents, which is the same twin-index shape
 * {@code DslFanOutIT} already uses for its DSL-off leg. Both legs run on this variant's own cluster.
 *
 * <p><b>Why only the {@code aggregations} section is compared.</b> {@code took} and {@code _shards} differ
 * legitimately between the two paths — the DSL path's {@code took} is conversion time and its shard counts
 * are hardcoded until the engine reports real ones — so a whole-body comparison would be red for reasons
 * that say nothing about assembly. {@code hits} likewise: every query here runs at {@code size: 0}.
 *
 * <p><b>Why the corpus is small, and why it must stay small.</b> {@code terms} {@code size} /
 * {@code min_doc_count} are not pushed down to the engine and the row stream truncates at 10k, both
 * recorded accepted risks. The fixture is therefore kept far under legacy's default top-10-per-level so
 * legacy's truncated view and the DSL's full cross product coincide. Widening it to chase parity on
 * {@code size} would be testing a deferred ticket, not this code.
 *
 * <p><b>Why the prices are what they are.</b> {@code price} is mapped {@code double} and every group's
 * values average exactly, so parity is an exact {@code Double} comparison rather than an
 * epsilon-tolerant one. {@code double} is also load-bearing for {@code sum}: a DSL {@code sum} over an
 * {@code integer}-mapped field declares its aggregate {@code INTEGER} while the plan's type system derives
 * {@code BIGINT} (see {@code AbstractMetricTranslator.toAggregateCall} against
 * {@code DslTypeSystem.deriveSumType}), which is a separate pre-existing defect this fixture deliberately
 * does not ride on.
 */
public abstract class DslParityTestBase extends AnalyticsRestTestCase {

    /** The parquet-primary index the DSL path is exercised over. */
    protected static final String DSL_INDEX = "dsl_parity";

    /** The plain-Lucene twin the legacy baseline comes from, with byte-identical documents. */
    protected static final String LEGACY_INDEX = "dsl_parity_lucene";

    protected static final String DSL_QUERY_ENABLED_KEY = "dsl.query.enabled";
    protected static final String MAX_PARALLEL_SUB_PLANS_KEY = "dsl.query.max_parallel_sub_plans";

    /** Provision once per JVM: both ITs in this hierarchy share the fixture. */
    private static boolean dataProvisioned = false;

    /**
     * 3 brands x 2 categories = 6 leaf groups, well under legacy's default {@code size: 10} per level, so
     * neither path drops a bucket. Prices are chosen so every group average is exact.
     */
    private static final String[][] DOCS = {
        // brand, category, price
        { "brand-a", "phone", "100.0" },
        { "brand-a", "phone", "300.0" },
        { "brand-a", "laptop", "500.0" },
        { "brand-b", "phone", "200.0" },
        { "brand-b", "laptop", "400.0" },
        { "brand-b", "laptop", "800.0" },
        { "brand-c", "phone", "50.0" },
        { "brand-c", "laptop", "150.0" },
        { "brand-c", "laptop", "250.0" } };

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createTwinIndices();
        indexDocs();
        dataProvisioned = true;
    }

    // ── Query bodies ───────────────────────────────────────────────────────

    /**
     * {@code terms(brand) > terms(category) > avg(price)} — the shape the whole feature exists for. Three
     * granularity levels come out of it, so the assembler has to join child buckets under the right parent
     * rather than render a flat list.
     */
    protected static String nestedAggBody() {
        return "{"
            + "\"size\": 0,"
            + "\"aggs\": {"
            + "  \"by_brand\": {"
            + "    \"terms\": { \"field\": \"brand\" },"
            + "    \"aggs\": {"
            + "      \"by_category\": {"
            + "        \"terms\": { \"field\": \"category\" },"
            + "        \"aggs\": { \"avg_price\": { \"avg\": { \"field\": \"price\" } } }"
            + "      }"
            + "    }"
            + "  }"
            + "}"
            + "}";
    }

    /**
     * {@code {"a": terms(brand){avg}, "b": terms(brand){sum}}} — two aggregations over the <b>same</b>
     * field. Before the aggregation name entered the granularity key these collapsed into one plan whose
     * metrics were merged, so a correct answer here is only possible with the agg-name-qualified key and a
     * map-keyed join. Legacy returns two independent aggregations, which is the reference.
     */
    protected static String siblingAggBody() {
        return "{"
            + "\"size\": 0,"
            + "\"aggs\": {"
            + "  \"a\": {"
            + "    \"terms\": { \"field\": \"brand\" },"
            + "    \"aggs\": { \"avg_price\": { \"avg\": { \"field\": \"price\" } } }"
            + "  },"
            + "  \"b\": {"
            + "    \"terms\": { \"field\": \"brand\" },"
            + "    \"aggs\": { \"total_price\": { \"sum\": { \"field\": \"price\" } } }"
            + "  }"
            + "}"
            + "}";
    }

    // ── Requests ───────────────────────────────────────────────────────────

    /**
     * Runs the body against an index and returns its {@code aggregations} section, bucket-order normalised.
     *
     * @param index the index to search
     * @param body the request body
     * @return the normalised aggregations section
     * @throws IOException if the request fails
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> aggregationsOf(String index, String body) throws IOException {
        Request search = new Request("POST", "/" + index + "/_search");
        search.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(search), "search " + index);

        Object aggregations = response.get("aggregations");
        assertNotNull("no aggregations section in the response from " + index + ": " + response, aggregations);
        Map<String, Object> normalised = (Map<String, Object>) aggregations;
        normalizeBucketsRecursive(normalised);
        return normalised;
    }

    /**
     * Sets a cluster-wide transient setting. {@code "null"} as the raw value clears the override, which is
     * what returns the node to its registered default — writing the literal default instead would hide a
     * default change.
     *
     * @param key the setting key
     * @param rawValue the JSON value, unquoted for booleans/numbers and {@code "null"} to clear
     * @throws IOException if the request fails
     */
    protected Response putTransient(String key, String rawValue) throws IOException {
        Request put = new Request("PUT", "/_cluster/settings");
        put.setJsonEntity("{\"transient\": {\"" + key + "\": " + rawValue + "}}");
        return client().performRequest(put);
    }

    // ── Normalisation ──────────────────────────────────────────────────────

    /**
     * Sorts every bucket list by key, recursively, so the comparison is order-insensitive.
     *
     * <p>Order-insensitive on purpose: bucket order within one {@code doc_count} tie is not part of what
     * parity is claiming, and the two paths break ties independently. Ordering IS asserted, separately and
     * deliberately, by the unit test over the terms leaf.
     *
     * @param aggMap an aggregations section, mutated in place
     */
    @SuppressWarnings("unchecked")
    protected static void normalizeBucketsRecursive(Map<String, Object> aggMap) {
        for (Map.Entry<String, Object> entry : aggMap.entrySet()) {
            if (entry.getValue() instanceof Map == false) {
                continue;
            }
            Map<String, Object> aggBody = (Map<String, Object>) entry.getValue();
            Object buckets = aggBody.get("buckets");
            if (buckets instanceof List == false) {
                continue;
            }
            List<Map<String, Object>> bucketList = (List<Map<String, Object>>) buckets;
            bucketList.sort(Comparator.comparing(bucket -> String.valueOf(bucket.get("key"))));
            for (Map<String, Object> bucket : bucketList) {
                normalizeBucketsRecursive(bucket);
            }
        }
    }

    // ── Fixture ────────────────────────────────────────────────────────────

    private void createTwinIndices() throws IOException {
        // Two shards, so the coordinator reduce stage is engaged rather than a single-shard shortcut that
        // would hide the cross-shard half of assembly.
        createIndexWithSettings(
            DSL_INDEX,
            "  \"number_of_shards\": 2,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": \"lucene\""
        );
        createIndexWithSettings(LEGACY_INDEX, "  \"number_of_shards\": 2," + "  \"number_of_replicas\": 0");
    }

    private void createIndexWithSettings(String index, String settings) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception e) {
            // index may not exist yet
        }

        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity(
            "{"
                + "\"settings\": {"
                + settings
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"brand\": { \"type\": \"keyword\" },"
                + "    \"category\": { \"type\": \"keyword\" },"
                + "    \"price\": { \"type\": \"double\" }"
                + "  }"
                + "}"
                + "}"
        );
        Map<String, Object> created = assertOkAndParse(client().performRequest(createIndex), "Create index " + index);
        assertEquals("Index creation should be acknowledged", true, created.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
    }

    private void indexDocs() throws IOException {
        for (String index : List.of(DSL_INDEX, LEGACY_INDEX)) {
            List<String> lines = new ArrayList<>();
            for (String[] doc : DOCS) {
                lines.add("{\"index\": {}}");
                lines.add("{\"brand\": \"" + doc[0] + "\", \"category\": \"" + doc[1] + "\", \"price\": " + doc[2] + "}");
            }
            Request bulk = new Request("POST", "/" + index + "/_bulk");
            bulk.setJsonEntity(String.join("\n", lines) + "\n");
            bulk.addParameter("refresh", "true");
            bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
            Map<String, Object> response = assertOkAndParse(client().performRequest(bulk), "Bulk index " + index);
            assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
        }
    }
}
