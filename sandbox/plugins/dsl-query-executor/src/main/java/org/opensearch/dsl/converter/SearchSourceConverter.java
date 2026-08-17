/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.aggregation.GranularityKeys;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Converts {@link SearchSourceBuilder} DSL into Calcite {@link QueryPlans}.
 *
 * <p>Builds its own Calcite planning infrastructure from the {@link SchemaPlus} provided
 * by the analytics engine.
 */
public class SearchSourceConverter {

    private final RelDataTypeFactory typeFactory;
    private final RexBuilder rexBuilder;
    private final CalciteCatalogReader catalogReader;
    private final FilterConverter filterConverter;
    private final ProjectConverter projectConverter;
    private final SortConverter sortConverter;
    private final AggregateConverter aggConverter;
    private final PostAggregateConverter postAggConverter;
    private final AggregationTreeWalker treeWalker;

    /**
     * Initializes planning infrastructure from the given schema.
     *
     * @param schema Calcite schema with index tables from the analytics engine
     */
    public SearchSourceConverter(SchemaPlus schema) {
        // TODO: Once Analytics plugin starts providing the RelOptTable, use it directly —
        // no need to reconstruct typeFactory, CatalogReader, and planning infrastructure here.
        // The RelOptCluster is deliberately NOT a field: it carries unguarded mutable metadata state
        // (mq / mqSupplier / metadataProvider) that every plan of a request would otherwise share, so
        // it is built per emitted plan in newBase(). Sharing the RexBuilder shares the type factory
        // into each per-plan cluster (RelOptCluster.create derives it from rexBuilder), which keeps
        // RelDataType instances interned across plans; every RexBuilder field is final.
        // DslTypeSystem, not RelDataTypeSystem.DEFAULT: the engine reduces this plan's AVG/STDDEV/VAR
        // into rule-generated SUM calls that infer their type through this factory's type system, and
        // the default one declares a sum as narrow as its input where the backend accumulates wider.
        // That disagreement is invisible on one shard and fails the query on two. See DslTypeSystem.
        this.typeFactory = new SqlTypeFactoryImpl(DslTypeSystem.INSTANCE);
        this.rexBuilder = new RexBuilder(typeFactory);

        CalciteSchema rootSchema = CalciteSchema.from(schema);
        this.catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );

        this.filterConverter = new FilterConverter(QueryRegistryFactory.create());
        this.projectConverter = new ProjectConverter();
        this.sortConverter = new SortConverter();
        this.aggConverter = new AggregateConverter();
        this.postAggConverter = new PostAggregateConverter();

        var aggRegistry = AggregationRegistryFactory.create();
        this.treeWalker = new AggregationTreeWalker(aggRegistry);
    }

    /**
     * Converts DSL for the given index into query plans.
     *
     * @param searchSource the DSL query
     * @param indexName target index
     * @return one or more query plans
     * @throws ConversionException if DSL conversion fails
     */
    public QueryPlans convert(SearchSourceBuilder searchSource, String indexName) throws ConversionException {
        RelOptTable table = catalogReader.getTable(List.of(indexName));
        if (table == null) {
            throw new IllegalArgumentException("Index not found in schema: " + indexName);
        }

        // Before a plan is emitted, because a pipeline aggregation produces none and would otherwise be
        // answered rather than rejected. See rejectPipelineAggregations.
        rejectPipelineAggregations(searchSource);

        int size = searchSource.size() != -1 ? searchSource.size() : SearchService.DEFAULT_SIZE;
        boolean hasAggs = hasAggregations(searchSource);

        QueryPlans.Builder builder = new QueryPlans.Builder();
        int emitted = 0;

        // Hits path: Scan → Filter → Project → Sort
        // size=0 skips hits — total doc count comes from analytics plugin metadata
        if (size > 0) {
            PlanBase hitsBase = newBase(table, searchSource);
            RelNode hits = projectConverter.convert(hitsBase.base(), hitsBase.ctx());
            hits = sortConverter.convert(hits, hitsBase.ctx());
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, hits, GranularityKeys.ROOT));
            emitted++;
        }

        // Aggregation path: Scan → Filter → Aggregate → PostAggregate (one per granularity level)
        if (hasAggs) {
            List<AggregationTreeWalker.Granularity> granularities = treeWalker.walk(
                searchSource.aggregations().getAggregatorFactories(),
                table.getRowType(),
                typeFactory
            );
            for (AggregationTreeWalker.Granularity granularity : granularities) {
                AggregationMetadata metadata = granularity.metadata();
                // One base per granularity, not per metric: the walker already merged same-granularity
                // metrics into one entry, so plan count stays equal to granularity count.
                PlanBase aggBase = newBase(table, searchSource);
                ConversionContext aggCtx = aggBase.ctx().withAggregationMetadata(metadata);
                RelNode aggs = aggConverter.convert(aggBase.base(), metadata);
                aggs = postAggConverter.convert(aggs, aggCtx);
                builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, aggs, granularity.key()));
                emitted++;
            }
        }

        // Translating the query clause is what validates it, and it only happens inside newBase(), so
        // a request that emits no plan (size=0 with no aggs) would never look at its query at all —
        // a malformed query would come back as an empty 200 instead of the conversion error the
        // pre-fan-out request path raised. Translate it once here and drop the result: the base is
        // fresh and unshared, so per-plan isolation is untouched.
        if (emitted == 0) {
            newBase(table, searchSource);
        }

        return builder.build();
    }

    /**
     * Builds the planning state for one emitted plan: its own {@link RelOptCluster}, its own
     * {@link ConversionContext} and its own {@code Scan → Filter} subtree.
     *
     * <p>Called once per emitted plan so that concurrent planning of a request's plans shares no
     * mutable Calcite state — and once, with the result discarded, for a request that emits no plan
     * at all, so its {@code query} clause is still translated and therefore still validated. What stays shared is provably immutable or thread-safe: the
     * {@link RexBuilder} (all fields final), its type factory (mutable state is static and
     * thread-safe) and the {@code table} — one {@link RelOptTable} identity across all plans, since
     * {@code catalogReader.getTable} is called once per request, outside this method, before any
     * fan-out. A fresh cluster also gives each plan its own {@code nextCorrel} /
     * {@code mapCorrelToRel} and its own {@code RelTraitSet} cache.
     *
     * <p>Package-private on purpose: no other component consumes it, and the per-plan isolation
     * assertions live in {@code SearchSourceConverterTests}.
     *
     * @param table the resolved Calcite table, shared by every plan of the request
     * @param searchSource the DSL query
     * @return this plan's context paired with its {@code Scan → Filter} subtree
     * @throws ConversionException if the {@code query} clause fails to convert
     */
    PlanBase newBase(RelOptTable table, SearchSourceBuilder searchSource) throws ConversionException {
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), rexBuilder);
        ConversionContext ctx = new ConversionContext(searchSource, cluster, table);
        RelNode scan = LogicalTableScan.create(cluster, table, List.of());
        return new PlanBase(ctx, filterConverter.convert(scan, ctx));
    }

    /**
     * One emitted plan's planning base.
     *
     * @param ctx the context every converter for this plan must be handed
     * @param base this plan's own {@code Scan → Filter} subtree
     */
    record PlanBase(ConversionContext ctx, RelNode base) {
    }

    /**
     * Rejects a request carrying a pipeline aggregation at any level of its aggregation tree.
     *
     * <p>Pipeline aggregations live in a <em>sibling</em> list of the one the walker reads:
     * {@code AggregatorFactories.Builder} keeps them in {@code getPipelineAggregatorFactories()} while
     * {@link AggregationTreeWalker#walk} is handed {@code getAggregatorFactories()} only. So a pipeline
     * aggregation reached no translator and raised no error — it simply produced no plan, and the response
     * came back without it. That was invisible while {@code SearchResponseBuilder} discarded every result
     * (the whole response was empty), but now that assembly renders the real {@code aggregations} section
     * a dropped pipeline aggregation would render as a plausible-looking answer with the requested
     * aggregation missing: a silently wrong response, which is the one outcome the assembly path refuses
     * everywhere else (a duplicate, missing or undecodable granularity key all fail loudly).
     *
     * <p>This is the existing unsupported-aggregation rejection reaching a list it could not see, not a
     * new feasibility classifier: {@code AggregationTreeWalker} already raises
     * {@link ConversionException} for any {@code AggregationBuilder} with no registered translator, and
     * no pipeline aggregation has one.
     *
     * @param searchSource the request body; a null body or a body with no aggregations has nothing to
     *     reject, and the size read below is what reports a null body
     * @throws ConversionException if any level of the aggregation tree carries a pipeline aggregation
     */
    private static void rejectPipelineAggregations(SearchSourceBuilder searchSource) throws ConversionException {
        if (searchSource == null || searchSource.aggregations() == null) {
            return;
        }
        rejectPipelineAggregations(
            searchSource.aggregations().getAggregatorFactories(),
            searchSource.aggregations().getPipelineAggregatorFactories()
        );
    }

    /** Descends one level of the request tree: this level's pipeline aggregations, then each sub-tree. */
    private static void rejectPipelineAggregations(Collection<AggregationBuilder> aggs, Collection<PipelineAggregationBuilder> pipelines)
        throws ConversionException {
        if (pipelines != null && pipelines.isEmpty() == false) {
            PipelineAggregationBuilder pipeline = pipelines.iterator().next();
            throw new ConversionException(
                "Pipeline aggregation '" + pipeline.getName() + "' of type '" + pipeline.getType() + "' is not supported"
            );
        }
        if (aggs == null) {
            return;
        }
        for (AggregationBuilder agg : aggs) {
            rejectPipelineAggregations(agg.getSubAggregations(), agg.getPipelineAggregations());
        }
    }

    private static boolean hasAggregations(SearchSourceBuilder searchSource) {
        return searchSource.aggregations() != null
            && searchSource.aggregations().getAggregatorFactories() != null
            && !searchSource.aggregations().getAggregatorFactories().isEmpty();
    }
}
