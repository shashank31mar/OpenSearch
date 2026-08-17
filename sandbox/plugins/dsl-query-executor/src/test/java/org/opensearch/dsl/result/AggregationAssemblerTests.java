/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregationAssemblerTests extends OpenSearchTestCase {

    private final AggregationAssembler assembler = new AggregationAssembler(AggregationRegistryFactory.create());

    public void testSingleLevelTermsWithAvgBuildsOneAggregation() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "avg_price", "_count"),
            row("BrandA", 850.0, 3L),
            row("BrandB", 1100.0, 2L)
        );

        StringTerms byBrand = assemble(List.of(result), source).get("by_brand");
        assertNotNull(byBrand);
        assertEquals(2, byBrand.getBuckets().size());

        Terms.Bucket brandA = bucketByKey(byBrand, "BrandA");
        assertEquals(3L, brandA.getDocCount());
        assertEquals(850.0, ((InternalAvg) brandA.getAggregations().get("avg_price")).getValue(), 0.0);
        Terms.Bucket brandB = bucketByKey(byBrand, "BrandB");
        assertEquals(2L, brandB.getDocCount());
        assertEquals(1100.0, ((InternalAvg) brandB.getAggregations().get("avg_price")).getValue(), 0.0);
    }

    public void testTwoLevelNestAttachesChildBucketsUnderParent() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new TermsAggregationBuilder("by_status").field("status")
                            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    )
            );
        ExecutionResult parent = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "_count"),
            row("BrandA", 3L),
            row("BrandB", 2L)
        );
        ExecutionResult child = aggResult(
            key(level("by_brand", "brand"), level("by_status", "status")),
            List.of("brand", "status", "avg_price", "_count"),
            row("BrandA", "new", 600.0, 2L),
            row("BrandA", "used", 250.0, 1L),
            row("BrandB", "new", 1100.0, 2L)
        );

        // Deliberately child-first: assembly is map-keyed on the granularity, so result order carries no
        // meaning — under fan-out plans complete in whatever order the engine finishes them.
        StringTerms byBrand = assemble(List.of(child, parent), source).get("by_brand");

        StringTerms brandAStatuses = bucketByKey(byBrand, "BrandA").getAggregations().get("by_status");
        assertEquals(List.of("new", "used"), keysOf(brandAStatuses));
        assertEquals(600.0, ((InternalAvg) bucketByKey(brandAStatuses, "new").getAggregations().get("avg_price")).getValue(), 0.0);

        StringTerms brandBStatuses = bucketByKey(byBrand, "BrandB").getAggregations().get("by_status");
        assertEquals("a child bucket must not leak across parents", List.of("new"), keysOf(brandBStatuses));
        assertEquals(2L, bucketByKey(brandBStatuses, "new").getDocCount());
    }

    public void testSiblingTermsOnSameFieldStayInSeparateAggregations() {
        // The D9 shape end-to-end: two aggregations over the SAME field, told apart only by the
        // aggregation name in the granularity key. This is the SC-7 seam in a unit test.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("a").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            )
            .aggregation(new TermsAggregationBuilder("b").field("brand").subAggregation(new SumAggregationBuilder("total").field("price")));
        ExecutionResult forA = aggResult(key(level("a", "brand")), List.of("brand", "avg_price", "_count"), row("BrandA", 850.0, 3L));
        ExecutionResult forB = aggResult(key(level("b", "brand")), List.of("brand", "total", "_count"), row("BrandA", 2550.0, 3L));

        InternalAggregations assembled = assembler.assemble(List.of(forA, forB), source);

        StringTerms a = assembled.get("a");
        StringTerms b = assembled.get("b");
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(850.0, ((InternalAvg) bucketByKey(a, "BrandA").getAggregations().get("avg_price")).getValue(), 0.0);
        assertEquals(2550.0, ((InternalSum) bucketByKey(b, "BrandA").getAggregations().get("total")).getValue(), 0.0);
        assertNull("a's metric must not leak into b", bucketByKey(b, "BrandA").getAggregations().get("avg_price"));
    }

    public void testAggNamePathSelectsResultNotBucketFieldName() {
        // Both results group by the same field; only the parsed aggName path can tell them apart, so a
        // field-name-based selection would resolve both requests to the same rows.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("a").field("brand"))
            .aggregation(new TermsAggregationBuilder("b").field("brand"));
        ExecutionResult forA = aggResult(key(level("a", "brand")), List.of("brand", "_count"), row("BrandA", 3L));
        ExecutionResult forB = aggResult(key(level("b", "brand")), List.of("brand", "_count"), row("BrandZ", 9L));

        InternalAggregations assembled = assembler.assemble(List.of(forA, forB), source);

        assertEquals(List.of("BrandA"), keysOf(assembled.get("a")));
        assertEquals(List.of("BrandZ"), keysOf(assembled.get("b")));
    }

    public void testDuplicateGranularityKeyThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("a").field("brand"));
        String duplicated = key(level("a", "brand"));
        ExecutionResult first = aggResult(duplicated, List.of("brand", "_count"), row("BrandA", 3L));
        ExecutionResult second = aggResult(duplicated, List.of("brand", "_count"), row("BrandB", 2L));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> assembler.assemble(List.of(first, second), source));
        assertTrue(e.getMessage(), e.getMessage().contains("share the granularity key"));
    }

    public void testMissingGranularityKeyThrows() {
        // An empty bucket list is indistinguishable from a legitimately empty result, so a missing result
        // must fail loudly rather than render one.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("a").field("brand"))
            .aggregation(new TermsAggregationBuilder("b").field("brand"));
        ExecutionResult onlyA = aggResult(key(level("a", "brand")), List.of("brand", "_count"), row("BrandA", 3L));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> assembler.assemble(List.of(onlyA), source));
        assertTrue(e.getMessage(), e.getMessage().contains("[b]"));
    }

    public void testMalformedGranularityKeyFailsAssembly() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("a").field("brand"));
        // Hand-corrupted length prefix: no positional fallback, no empty aggregation — the parse throw
        // propagates and the request fails.
        ExecutionResult corrupted = aggResult("0:99#a:7#5#brand", List.of("brand", "_count"), row("BrandA", 3L));

        expectThrows(IllegalArgumentException.class, () -> assembler.assemble(List.of(corrupted), source));
    }

    public void testNullMetricValueIsRendered() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "avg_price", "_count"),
            row("BrandA", null, 3L)
        );

        StringTerms byBrand = assemble(List.of(result), source).get("by_brand");
        InternalAvg avg = bucketByKey(byBrand, "BrandA").getAggregations().get("avg_price");
        assertNotNull(avg);
        assertTrue("a null metric renders the empty convention, not 0", Double.isNaN(avg.getValue()));
    }

    public void testImplicitCountColumnBecomesDocCount() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"));
        ExecutionResult result = aggResult(key(level("by_brand", "brand")), List.of("brand", "_count"), row("BrandA", 7L));

        StringTerms byBrand = assemble(List.of(result), source).get("by_brand");
        Terms.Bucket brandA = bucketByKey(byBrand, "BrandA");
        assertEquals(7L, brandA.getDocCount());
        // _count is doc_count, never a metric aggregation of its own.
        assertNull(brandA.getAggregations().get("_count"));
        assertEquals(0, brandA.getAggregations().asList().size());
    }

    public void testMetricColumnsResolvedByNameNotPosition() {
        // The plan's row type puts _count first and the metrics in the opposite order from the request.
        // Any index arithmetic lands the values on the wrong metric; resolution by name does not.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    .subAggregation(new SumAggregationBuilder("total").field("price"))
            );
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("_count", "total", "brand", "avg_price"),
            row(3L, 2550.0, "BrandA", 850.0)
        );

        Terms.Bucket brandA = bucketByKey(assemble(List.of(result), source).get("by_brand"), "BrandA");
        assertEquals(3L, brandA.getDocCount());
        assertEquals(850.0, ((InternalAvg) brandA.getAggregations().get("avg_price")).getValue(), 0.0);
        assertEquals(2550.0, ((InternalSum) brandA.getAggregations().get("total")).getValue(), 0.0);
    }

    public void testGroupByColumnsComeFromTheParsedGranularityKey() {
        // The results carry ONLY a plan and rows — no AggregationMetadata exists anywhere in this test —
        // so the bucket key columns can only have come from parsing the granularity key.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new TermsAggregationBuilder("by_status").field("status"))
            );
        ExecutionResult parent = aggResult(key(level("by_brand", "brand")), List.of("brand", "_count"), row("BrandA", 3L));
        ExecutionResult child = aggResult(
            key(level("by_brand", "brand"), level("by_status", "status")),
            List.of("brand", "status", "_count"),
            row("BrandA", "new", 3L)
        );

        StringTerms byBrand = assemble(List.of(parent, child), source).get("by_brand");
        assertEquals(List.of("BrandA"), keysOf(byBrand));
        // The child bucket's key is its OWN level's column ("status"), not the whole group-by prefix.
        StringTerms byStatus = bucketByKey(byBrand, "BrandA").getAggregations().get("by_status");
        assertEquals(List.of("new"), keysOf(byStatus));
    }

    public void testRepeatedColumnNameFailsInsteadOfBindingTheWrongValue() {
        // A metric named exactly _count is a legal aggregation name, and keeping the FIRST column with a
        // given name bound doc_count to that metric: every bucket reported the metric's value as its
        // document count. AggregationMetadataBuilder rejects the request before a plan is built; if a row
        // type with a repeated name ever reaches here anyway it must fail, not answer with a wrong number.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new SumAggregationBuilder("_count").field("price"))
            );
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "_count", "_count"),
            row("BrandA", 2550.0, 3L)
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> assembler.assemble(List.of(result), source));
        assertTrue(e.getMessage(), e.getMessage().contains("repeats column [_count]"));
    }

    /**
     * The thrown message reaches the caller — {@code TransportDslExecuteAction} hands a build failure to
     * {@code onFailure} and OpenSearch renders the message into the error body — so it must name the one
     * column that is missing and enumerate no other column of the queried index. The full row type is the
     * operator's, via the log.
     */
    public void testMissingColumnMessageNamesOnlyTheMissingColumn() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"));
        // A row type with no _count column: doc_count resolution throws while the other columns resolve.
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "salary_band", "national_id"),
            row("BrandA", "senior", "NID-1")
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> assembler.assemble(List.of(result), source));
        assertTrue(e.getMessage(), e.getMessage().contains("expects column [_count]"));
        assertFalse("the caller-facing message must not enumerate the row type: " + e.getMessage(), e.getMessage().contains("salary_band"));
        assertFalse(e.getMessage(), e.getMessage().contains("national_id"));
    }

    /** Same rule on the duplicate-column throw: the offending column, not the whole row type. */
    public void testRepeatedColumnMessageNamesOnlyTheRepeatedColumn() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new SumAggregationBuilder("_count").field("price"))
            );
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "salary_band", "_count", "_count"),
            row("BrandA", "senior", 2550.0, 3L)
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> assembler.assemble(List.of(result), source));
        assertTrue(e.getMessage(), e.getMessage().contains("repeats column [_count]"));
        assertFalse("the caller-facing message must not enumerate the row type: " + e.getMessage(), e.getMessage().contains("salary_band"));
    }

    public void testGroupByColumnRepeatedAsAMetricNameFails() {
        // Same hole from the other side: a metric named after this level's grouping field. Resolving it by
        // name would read the bucket key as the metric value.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new SumAggregationBuilder("brand").field("price"))
            );
        ExecutionResult result = aggResult(key(level("by_brand", "brand")), List.of("brand", "brand", "_count"), row("BrandA", 2550.0, 3L));

        expectThrows(IllegalStateException.class, () -> assembler.assemble(List.of(result), source));
    }

    public void testNestedAssemblyGroupsChildRowsOnceRatherThanRescanningPerParent() {
        // The child result is scanned once and grouped by its parent prefix. Pinning the OUTCOME rather
        // than the scan count: every parent must still get exactly its own children, with no leakage and no
        // duplication, which is what a per-parent re-scan produced correctly but quadratically.
        // size(40) so the terms leaf's default size of 10 does not truncate what this test is about.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .size(40)
                    .subAggregation(new TermsAggregationBuilder("by_status").field("status"))
            );
        List<Object[]> parentRows = new ArrayList<>();
        List<Object[]> childRows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            parentRows.add(row("brand-" + i, 2L));
            childRows.add(row("brand-" + i, "new", 1L));
            childRows.add(row("brand-" + i, "used", 1L));
        }
        ExecutionResult parent = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "_count"),
            parentRows.toArray(Object[][]::new)
        );
        ExecutionResult child = aggResult(
            key(level("by_brand", "brand"), level("by_status", "status")),
            List.of("brand", "status", "_count"),
            childRows.toArray(Object[][]::new)
        );

        StringTerms byBrand = assemble(List.of(parent, child), source).get("by_brand");

        assertEquals(40, byBrand.getBuckets().size());
        for (Terms.Bucket bucket : byBrand.getBuckets()) {
            StringTerms statuses = bucket.getAggregations().get("by_status");
            assertEquals("each parent keeps exactly its own children", List.of("new", "used"), keysOf(statuses));
        }
    }

    public void testUngroupedMetricComesFromRootGranularity() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        ExecutionResult result = aggResult(GranularityKeys.ROOT, List.of("avg_price"), row(725.0));

        InternalAvg avg = assembler.assemble(List.of(result), source).get("avg_price");
        assertEquals(725.0, avg.getValue(), 0.0);
    }

    public void testNoRequestedAggregationsYieldsNoSection() {
        assertNull(assembler.assemble(List.of(), new SearchSourceBuilder()));
        assertNull(assembler.assemble(List.of(), null));
    }

    public void testAssemblerDoesNotTouchRelMetadataQuery() {
        // G6: assembly runs on the engine's completion thread, which never sets THREAD_PROVIDERS, so any
        // RelMetadataQuery materialised here NPEs. Nothing in sandbox/ ever clears the ThreadLocal, so a
        // warm pooled thread would make this vacuous — clear it first (G5), and build the fixture BEFORE
        // clearing so the fixture itself cannot be what primes it.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
        ExecutionResult result = aggResult(
            key(level("by_brand", "brand")),
            List.of("brand", "avg_price", "_count"),
            row("BrandA", 850.0, 3L)
        );

        RelMetadataQueryBase.THREAD_PROVIDERS.remove();
        assertNull("the assembling thread must start cold", RelMetadataQueryBase.THREAD_PROVIDERS.get());

        StringTerms byBrand = assemble(List.of(result), source).get("by_brand");

        assertEquals(1, byBrand.getBuckets().size());
        assertNull("assembly must not prime THREAD_PROVIDERS either", RelMetadataQueryBase.THREAD_PROVIDERS.get());
    }

    // ---- Helpers ----

    private InternalAggregations assemble(List<ExecutionResult> results, SearchSourceBuilder source) {
        InternalAggregations assembled = assembler.assemble(results, source);
        assertNotNull(assembled);
        return assembled;
    }

    private static FieldGrouping level(String aggName, String... fieldNames) {
        return new FieldGrouping(aggName, List.of(fieldNames));
    }

    private static String key(GroupingInfo... levels) {
        return GranularityKeys.granularityKey(List.of(levels));
    }

    private static Object[] row(Object... values) {
        return values;
    }

    private static ExecutionResult aggResult(String granularity, List<String> columnNames, Object[]... rows) {
        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(
            QueryPlans.Type.AGGREGATION,
            TestUtils.createRelNodeWithColumns(columnNames),
            granularity
        );
        return new ExecutionResult(plan, Arrays.asList(rows));
    }

    private static Terms.Bucket bucketByKey(Terms terms, String key) {
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (bucket.getKeyAsString().equals(key)) {
                return bucket;
            }
        }
        throw new AssertionError("no bucket [" + key + "] in " + keysOf(terms));
    }

    private static List<String> keysOf(Terms terms) {
        List<String> keys = new ArrayList<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            keys.add(bucket.getKeyAsString());
        }
        return keys;
    }
}
