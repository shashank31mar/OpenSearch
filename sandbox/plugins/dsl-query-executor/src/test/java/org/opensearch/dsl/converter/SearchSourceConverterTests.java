/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public class SearchSourceConverterTests extends OpenSearchTestCase {

    /** Invalidate/get rounds each plan's thread runs against its own cluster. */
    private static final int METADATA_ROUNDS = 200;

    /** Bound on the start barrier so a stuck worker fails the test instead of hanging it. */
    private static final int BARRIER_TIMEOUT_SECONDS = 30;

    /** Bound on joining each worker. */
    private static final int JOIN_TIMEOUT_SECONDS = 60;

    private SearchSourceConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                // Nullable fields — matches OpenSearchSchemaBuilder behavior
                return typeFactory.builder()
                    .add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("brand", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("rating", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true))
                    .build();
            }
        });
        converter = new SearchSourceConverter(schema);
    }

    public void testConvertProducesHitsPlan() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertTrue(plan.relNode() instanceof LogicalTableScan);
    }

    public void testConvertResolvesFieldNames() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertEquals(4, plan.relNode().getRowType().getFieldCount());
        assertEquals(List.of("name", "price", "brand", "rating"), plan.relNode().getRowType().getFieldNames());
    }

    public void testConvertThrowsForMissingIndex() {
        expectThrows(IllegalArgumentException.class, () -> converter.convert(new SearchSourceBuilder(), "nonexistent-index"));
    }

    public void testAggsWithSizeZeroProducesOnlyAggregationPlan() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(1, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testAggsWithSizeGreaterThanZeroProducesBothPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(10).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testNoAggsProducesOnlyHitsPlan() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testSizeZeroNoAggsProducesNoPlans() throws ConversionException {
        // size=0 with no aggs produces no plans — total doc count comes from analytics plugin metadata
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(0, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testSizeZeroNoAggsWithInvalidQueryThrowsInsteadOfZeroPlans() {
        // The zero-plan path still has to translate the query clause, because translating it is what
        // validates it: an unknown field must surface as a ConversionException (which the transport
        // turns into a failure response) instead of an empty, indistinguishable "no results" answer.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).query(QueryBuilders.termQuery("nope", "x"));

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertThat(e.getMessage(), containsString("nope"));
    }

    public void testSizeZeroNoAggsWithValidQueryStillProducesNoPlans() throws ConversionException {
        // The guard above must not turn a valid query into an error: a resolvable query clause on the
        // zero-plan path is still a normal empty result, not a failure.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).query(QueryBuilders.termQuery("brand", "acme"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(0, plans.getAll().size());
    }

    public void testAggPlanIncludesPostAggSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.key(true))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
        QueryPlans plans = converter.convert(source, "test-index");

        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        // Aggregation plan should be wrapped with LogicalSort for bucket order
        assertTrue(plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode() instanceof LogicalSort);
    }

    public void testMetricOnlyAggPlanHasNoPostAggSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        // Metric-only agg has no bucket orders, so no LogicalSort wrapper
        assertFalse(plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode() instanceof LogicalSort);
    }

    public void testHitsPlanCarriesRootGranularity() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(GranularityKeys.ROOT, plans.get(QueryPlans.Type.HITS).get(0).granularity());
    }

    public void testAggregationPlansCarryDistinctGranularities() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new AvgAggregationBuilder("brand_avg").field("price"))
                    .subAggregation(
                        new TermsAggregationBuilder("by_name").field("name")
                            .subAggregation(new AvgAggregationBuilder("name_avg").field("price"))
                    )
            );
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());
        String parent = aggPlans.get(0).granularity();
        String child = aggPlans.get(1).granularity();
        assertNotEquals(parent, child);
        // The shallower key is a strict prefix of the deeper one, which is what lets response
        // assembly attach child buckets under their parent. (GranularityKeys.isAncestorKey, which
        // asserts the same property including the level boundary, arrives with the assembler.)
        assertTrue(parent + " should be a strict prefix of " + child, child.startsWith(parent));
        assertNotEquals(GranularityKeys.ROOT, parent);
    }

    public void testSiblingAggregationsOnSameFieldYieldTwoPlans() throws ConversionException {
        // Two aggregations over the same field are two granularities: a field-only granularity key
        // merged them into a single plan and lost bucket identity.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("a").field("brand").subAggregation(new AvgAggregationBuilder("avg").field("price")))
            .aggregation(new TermsAggregationBuilder("b").field("brand").subAggregation(new AvgAggregationBuilder("avg2").field("rating")));
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());
        assertNotEquals(aggPlans.get(0).granularity(), aggPlans.get(1).granularity());
    }

    // ---- Pipeline aggregations: rejected, never silently dropped ----

    public void testPipelineAggregationIsRejected() {
        // A pipeline aggregation lives in a sibling list the walker never reads, so it used to produce no
        // plan and no error: the response came back without it. Now that assembly renders the real
        // aggregations section, that would be a plausible-looking response with the requested
        // aggregation simply missing.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            )
            .aggregation(new MaxBucketPipelineAggregationBuilder("max_avg", "by_brand>avg_price"));

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertThat(e.getMessage(), containsString("max_avg"));
        assertThat(e.getMessage(), containsString("max_bucket"));
    }

    public void testNestedPipelineAggregationIsRejected() {
        // Nested is the harder half: the top-level list is clean, so only a descent into each
        // sub-aggregation's own pipeline list catches it.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    .subAggregation(new MaxBucketPipelineAggregationBuilder("max_avg", "avg_price"))
            );

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertThat(e.getMessage(), containsString("max_avg"));
    }

    public void testPipelineAggregationAloneIsRejectedRatherThanAnsweredAsAHitsQuery() {
        // With no ordinary aggregation beside it, getAggregatorFactories() is empty, so the aggregation
        // path is skipped entirely and the request would have been answered as a plain hits query.
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            new MaxBucketPipelineAggregationBuilder("max_avg", "by_brand>avg_price")
        );

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertThat(e.getMessage(), containsString("max_avg"));
    }

    // ---- Per-plan planning isolation (SC-5) ----

    public void testEachPlanHasItsOwnCluster() throws ConversionException {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals("expected 1 HITS + 2 AGGREGATION plans", 3, all.size());

        for (int i = 0; i < all.size(); i++) {
            for (int j = i + 1; j < all.size(); j++) {
                assertNotSame(
                    "plans " + i + " and " + j + " share one RelOptCluster",
                    all.get(i).relNode().getCluster(),
                    all.get(j).relNode().getCluster()
                );
            }
        }

        Set<RelOptCluster> clusters = Collections.newSetFromMap(new IdentityHashMap<>());
        all.forEach(plan -> clusters.add(plan.relNode().getCluster()));
        assertEquals("one cluster per plan", all.size(), clusters.size());
    }

    public void testNewBaseReturnsFreshClusterCtxAndSubtreePerCall() throws ConversionException {
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("test-index", testIndexMapping());
        SearchSourceBuilder source = nestedThreeLevelSource();

        SearchSourceConverter.PlanBase first = converter.newBase(infra.table(), source);
        SearchSourceConverter.PlanBase second = converter.newBase(infra.table(), source);

        // The query clause must have been applied per call — otherwise the "fresh subtree" assertion
        // below would only be about the scan and would not cover the shared Scan → Filter base.
        assertTrue("newBase must apply the query clause", first.base() instanceof LogicalFilter);

        assertNotSame(first.ctx(), second.ctx());
        assertNotSame(first.ctx().getCluster(), second.ctx().getCluster());
        assertNotSame(first.base(), second.base());

        // Deliberately shared: one RexBuilder across the per-plan clusters, whose fields are all
        // final. The other shared piece — one RelOptTable identity per request — cannot be asserted
        // here: this test hands both calls the same table, so any such assertion would hold even if
        // convert() resolved a fresh table per plan. testAllPlansShareOneRelOptTable proves it from
        // the plans convert() actually emits.
        assertSame(first.ctx().getRexBuilder(), second.ctx().getRexBuilder());
    }

    public void testNoIdentitySharedRelNodesBetweenPlans() throws ConversionException {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals(3, all.size());

        List<Set<RelNode>> nodesPerPlan = all.stream().map(plan -> collectNodes(plan.relNode())).collect(Collectors.toList());
        for (Set<RelNode> nodes : nodesPerPlan) {
            // A shared base would show up as a shared LogicalFilter, so the walk has to reach one.
            assertTrue("plan has no LogicalFilter to share", nodes.stream().anyMatch(n -> n instanceof LogicalFilter));
        }

        for (int i = 0; i < nodesPerPlan.size(); i++) {
            for (int j = i + 1; j < nodesPerPlan.size(); j++) {
                Set<RelNode> shared = Collections.newSetFromMap(new IdentityHashMap<>());
                shared.addAll(nodesPerPlan.get(i));
                shared.retainAll(nodesPerPlan.get(j));
                assertTrue("plans " + i + " and " + j + " share RelNodes: " + shared, shared.isEmpty());
            }
        }
    }

    public void testAllPlansShareOneRelOptTable() throws ConversionException {
        // SC-5's shared half: catalogReader.getTable runs once per request, so every plan of the
        // request scans the same RelOptTable instance. The tables are read back out of the emitted
        // RelNodes — nothing here is handed to the converter, so resolving a table per plan (each
        // getTable call builds a new RelOptTableImpl) makes this fail.
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals("expected 1 HITS + 2 AGGREGATION plans", 3, all.size());

        Set<RelOptTable> tables = Collections.newSetFromMap(new IdentityHashMap<>());
        for (QueryPlans.QueryPlan plan : all) {
            Set<RelOptTable> planTables = scanTablesOf(plan.relNode());
            // Every plan must reach a scan, or "same table everywhere" would hold over nothing.
            assertFalse("plan " + plan.granularity() + " has no TableScan", planTables.isEmpty());
            tables.addAll(planTables);
        }

        assertEquals("one RelOptTable identity across all plans, but got " + tables, 1, tables.size());
        RelOptTable shared = tables.iterator().next();
        for (QueryPlans.QueryPlan plan : all) {
            for (RelOptTable table : scanTablesOf(plan.relNode())) {
                assertSame("every plan must scan the one RelOptTable resolved once per request", shared, table);
            }
        }
    }

    /**
     * Replays, on all plans of one request at once, the per-plan pair of calls the engine makes at
     * {@code DefaultPlanExecutor}: set {@code THREAD_PROVIDERS}, then invalidate that plan's metadata
     * query. Pre-isolation the plans share one {@code RelOptCluster} whose {@code mq} field is neither
     * volatile nor guarded, so one thread's {@code invalidateMetadataQuery()} can land inside another's
     * unsynchronized lazy init and make {@code getMetadataQuery()} return null.
     *
     * <p>The failure mode is seed-dependent (NPE / corrupted metadata table / spurious
     * {@code CyclicMetadataException}), so the assertion is only "no throwable escaped".
     */
    public void testConcurrentMetadataAccessAcrossPlansIsIsolated() throws Exception {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals(3, all.size());

        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        CyclicBarrier barrier = new CyclicBarrier(all.size());
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            RelNode relNode = all.get(i).relNode();
            threads.add(new Thread(() -> {
                try {
                    // G5: a pooled thread that already carries a provider would mask a missing set()
                    // and make this probe vacuous.
                    RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                    RelOptCluster cluster = relNode.getCluster();
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    for (int round = 0; round < METADATA_ROUNDS; round++) {
                        RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(cluster.getMetadataProvider()));
                        cluster.invalidateMetadataQuery();
                        RelMdUtil.clearCache(relNode);
                        assertNotNull(cluster.getMetadataQuery().getRowCount(relNode));
                    }
                } catch (Throwable t) {
                    errors.offer(t);
                } finally {
                    RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                }
            }, "dsl-plan-metadata-" + i));
        }

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(JOIN_TIMEOUT_SECONDS));
            assertFalse("worker did not finish: " + thread.getName(), thread.isAlive());
        }
        assertTrue(errors.toString(), errors.isEmpty());
    }

    public void testConversionFailureOnUnknownFieldPropagates() {
        // Filter conversion now runs once per emitted plan; a per-plan failure must still surface as
        // one exception out of convert() rather than being swallowed by the emit loop.
        SearchSourceBuilder source = new SearchSourceBuilder().size(10)
            .query(QueryBuilders.termQuery("nope", "x"))
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg").field("price"))
            );

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertThat(e.getMessage(), containsString("nope"));
    }

    /** The 3-level nested source that emits 1 HITS + 2 AGGREGATION plans. */
    private static SearchSourceBuilder nestedThreeLevelSource() {
        return new SearchSourceBuilder().size(10)
            .query(QueryBuilders.termQuery("brand", "acme"))
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new TermsAggregationBuilder("by_name").field("name")
                            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    )
            );
    }

    /** The same four fields {@link #setUp} registers, for building a standalone {@link RelOptTable}. */
    private static Map<String, String> testIndexMapping() {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("name", "VARCHAR");
        mapping.put("price", "INTEGER");
        mapping.put("brand", "VARCHAR");
        mapping.put("rating", "DOUBLE");
        return mapping;
    }

    /** Collects a plan's RelNodes into an identity set. */
    private static Set<RelNode> collectNodes(RelNode root) {
        Set<RelNode> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<RelNode> pending = new ArrayDeque<>();
        pending.push(root);
        while (!pending.isEmpty()) {
            RelNode current = pending.pop();
            if (nodes.add(current)) {
                current.getInputs().forEach(pending::push);
            }
        }
        return nodes;
    }

    /** Returns the identity set of tables scanned anywhere in a plan (all branches, not just input 0). */
    private static Set<RelOptTable> scanTablesOf(RelNode root) {
        Set<RelOptTable> tables = Collections.newSetFromMap(new IdentityHashMap<>());
        for (RelNode node : collectNodes(root)) {
            if (node instanceof TableScan) {
                tables.add(node.getTable());
            }
        }
        return tables;
    }

    // ---- Golden file driven RelNode generation tests ----

    /**
     * Auto-discovers all golden JSON files and validates that each inputDsl
     * produces the expected RelNode plan via SearchSourceConverter.convert().
     * Adding a new test case only requires adding a new JSON file — no new
     * Java method needed.
     */
    public void testGoldenFileRelNodeGeneration() throws Exception {
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

                SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
                SearchSourceConverter conv = new SearchSourceConverter(infra.schema());
                QueryPlans plans = conv.convert(searchSource, tc.getIndexName());

                QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
                List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
                if (matchingPlans.isEmpty()) {
                    failures.add(fileName + ": No " + expectedType + " plan produced");
                    continue;
                }

                RelNode relNode = matchingPlans.get(0).relNode();
                String actualPlan = relNode.explain().trim();
                String expectedPlan = String.join("\n", tc.getExpectedRelNodePlan());

                if (!expectedPlan.equals(actualPlan)) {
                    failures.add(fileName + ": RelNode plan mismatch\n  Expected: " + expectedPlan + "\n  Actual:   " + actualPlan);
                }

                List<String> actualFields = relNode.getRowType().getFieldNames();
                if (!tc.getMockResultFieldNames().equals(actualFields)) {
                    failures.add(
                        fileName + ": Field names mismatch\n  Expected: " + tc.getMockResultFieldNames() + "\n  Actual:   " + actualFields
                    );
                }
            } catch (Exception e) {
                failures.add(fileName + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            fail("Golden file RelNode generation failures:\n" + String.join("\n", failures));
        }
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
}
