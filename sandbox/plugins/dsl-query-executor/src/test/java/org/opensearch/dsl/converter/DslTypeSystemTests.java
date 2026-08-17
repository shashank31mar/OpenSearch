/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;

/**
 * Pins the one way {@link DslTypeSystem} differs from {@link RelDataTypeSystem#DEFAULT}: the width a
 * {@code SUM} is declared with.
 *
 * <p>The load-bearing test here is {@link #testReducedAvgSumIsBigintOnAnIntegerField()} — it runs the
 * same aggregate-reduction the analytics engine runs over a DSL plan and asserts the rule-generated
 * {@code SUM} (the {@code $f2} of the multi-shard exchange failure) comes out as {@code BIGINT}. The
 * per-family tests below are the unit pins under it; deleting either half leaves the regression
 * uncovered, because the reduction test alone would not say which families are widened and the
 * per-family tests alone would not prove the type system reaches the emitted plan.
 */
public class DslTypeSystemTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new SqlTypeFactoryImpl(DslTypeSystem.INSTANCE);
    }

    /**
     * The failure this whole class exists for: on a {@code price: integer} field the engine's
     * {@code OpenSearchAggregateReduceRule} turns the DSL's {@code AVG} into an unnamed {@code SUM} plus
     * an unnamed {@code COUNT}. With Calcite's default type system that {@code SUM} is declared
     * {@code INTEGER} while the DataFusion backend accumulates it in {@code Int64}, and on a 2-shard
     * index the cross-fragment schema check rejects the plan
     * ({@code Field '$f2' ... (Int32) ... table schema (Int64)}). Asserting {@code BIGINT} here is
     * asserting that plan is accepted.
     *
     * <p>Uses Calcite's own {@code AggregateReduceFunctionsRule}, configured with the engine's exact
     * {@code FUNCTIONS_TO_REDUCE} set, rather than a hand-built call: the mismatching {@code SUM} is
     * written by that rule, not by this plugin, so the test has to reproduce the rule's inference rather
     * than a guess at it. (Calcite's own {@code CoreRules.AGGREGATE_REDUCE_FUNCTIONS} default set also
     * reduces {@code SUM} itself into {@code $SUM0}, which the engine's set deliberately does not.)
     */
    public void testReducedAvgSumIsBigintOnAnIntegerField() throws ConversionException {
        RelNode aggPlan = nestedAvgAggregationPlan();

        Aggregate reduced = reduceAggregateFunctions(aggPlan);
        AggregateCall sum = onlyCallOfKind(reduced, SqlKind.SUM);

        assertEquals(
            "the reduced AVG's SUM must be declared as wide as the engine accumulates it, or the "
                + "2-shard exchange schema check rejects the plan",
            SqlTypeName.BIGINT,
            sum.getType().getSqlTypeName()
        );
        // The count half is already i64 on both sides; asserted so a change that narrows either half of
        // the decomposition is caught here rather than on a cluster.
        for (AggregateCall count : callsOfKind(reduced, SqlKind.COUNT)) {
            assertEquals("every COUNT of the decomposition must stay i64", SqlTypeName.BIGINT, count.getType().getSqlTypeName());
        }
    }

    /**
     * The contrast half of the test above: with Calcite's default type system the same field yields
     * {@code INTEGER}. Without this, a reader cannot tell whether the assertion above is pinning
     * anything.
     */
    public void testDefaultTypeSystemWouldDeclareTheSumAsInteger() {
        RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType integer = nullable(defaultFactory, SqlTypeName.INTEGER);

        assertEquals(SqlTypeName.INTEGER, defaultFactory.getTypeSystem().deriveSumType(defaultFactory, integer).getSqlTypeName());
    }

    public void testSignedIntegerFamilyWidensToBigint() {
        for (SqlTypeName narrower : List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
            RelDataType sumType = deriveSum(nullable(typeFactory, narrower));
            assertEquals(narrower + " must sum as BIGINT", SqlTypeName.BIGINT, sumType.getSqlTypeName());
        }
    }

    public void testApproximateNumericFamilyWidensToDouble() {
        for (SqlTypeName approximate : List.of(SqlTypeName.REAL, SqlTypeName.FLOAT, SqlTypeName.DOUBLE)) {
            RelDataType sumType = deriveSum(nullable(typeFactory, approximate));
            assertEquals(approximate + " must sum as DOUBLE", SqlTypeName.DOUBLE, sumType.getSqlTypeName());
        }
    }

    /**
     * Nullability is part of the sum type, not an afterthought: Calcite's empty-group handling reads it,
     * so a widening that returned a NOT NULL type would change results, not just the declared width.
     */
    public void testNullabilityIsCarriedFromTheArgument() {
        assertTrue(deriveSum(nullable(typeFactory, SqlTypeName.INTEGER)).isNullable());
        assertFalse(deriveSum(typeFactory.createSqlType(SqlTypeName.INTEGER)).isNullable());
    }

    /** A family with no known accumulator widening falls through to Calcite, never to a guess. */
    public void testUnknownFamilyFallsBackToCalciteDefault() {
        RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        for (SqlTypeName untouched : List.of(SqlTypeName.DECIMAL, SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN)) {
            RelDataType argument = nullable(typeFactory, untouched);
            assertEquals(
                untouched + " must keep Calcite's derivation",
                defaultFactory.getTypeSystem().deriveSumType(defaultFactory, argument).getSqlTypeName(),
                deriveSum(argument).getSqlTypeName()
            );
        }
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    private RelDataType deriveSum(RelDataType argumentType) {
        return typeFactory.getTypeSystem().deriveSumType(typeFactory, argumentType);
    }

    private static RelDataType nullable(RelDataTypeFactory factory, SqlTypeName typeName) {
        return factory.createTypeWithNullability(factory.createSqlType(typeName), true);
    }

    /**
     * The AGGREGATION plan of {@code terms(brand) > terms(name) > avg(price)} over the mapping the
     * failing 2-shard IT provisions: {@code price} integer, {@code brand}/{@code name} keyword,
     * {@code rating} double. Built through the real converter, so the plan carries the real cluster and
     * therefore the real type factory.
     */
    private static RelNode nestedAvgAggregationPlan() throws ConversionException {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder()
                    .add("name", nullable(factory, SqlTypeName.VARCHAR))
                    .add("price", nullable(factory, SqlTypeName.INTEGER))
                    .add("brand", nullable(factory, SqlTypeName.VARCHAR))
                    .add("rating", nullable(factory, SqlTypeName.DOUBLE))
                    .build();
            }
        });
        SearchSourceBuilder source = new SearchSourceBuilder().size(10)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new TermsAggregationBuilder("by_name").field("name")
                            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    )
            );
        QueryPlans plans = new SearchSourceConverter(schema).convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggregations = plans.get(QueryPlans.Type.AGGREGATION);
        assertFalse("the fixture must emit an aggregation plan", aggregations.isEmpty());
        // The deepest granularity is the one carrying avg(price).
        return aggregations.get(aggregations.size() - 1).relNode();
    }

    /**
     * Runs the reduction the engine runs ({@code AVG} to {@code SUM}/{@code COUNT}) and returns the
     * aggregate. The rule is built with the same three arguments {@code OpenSearchAggregateReduceRule}
     * passes, so the reduction under test is the production one.
     */
    private static Aggregate reduceAggregateFunctions(RelNode plan) {
        RelOptRule reduceRule = new AggregateReduceFunctionsRule(
            LogicalAggregate.class,
            RelBuilder.proto(Contexts.empty()),
            EnumSet.of(SqlKind.AVG, SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP, SqlKind.VAR_POP, SqlKind.VAR_SAMP)
        );
        HepPlanner planner = new HepPlanner(HepProgram.builder().addRuleInstance(reduceRule).build());
        planner.setRoot(plan);
        return findAggregate(planner.findBestExp());
    }

    private static Aggregate findAggregate(RelNode root) {
        Deque<RelNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (queue.isEmpty() == false) {
            RelNode node = queue.removeFirst();
            if (node instanceof Aggregate aggregate) {
                return aggregate;
            }
            queue.addAll(node.getInputs());
        }
        throw new AssertionError("no Aggregate in the reduced plan: " + root);
    }

    private static AggregateCall onlyCallOfKind(Aggregate aggregate, SqlKind kind) {
        List<AggregateCall> matching = callsOfKind(aggregate, kind);
        assertEquals("expected exactly one " + kind + " in " + aggregate.getAggCallList(), 1, matching.size());
        return matching.get(0);
    }

    private static List<AggregateCall> callsOfKind(Aggregate aggregate, SqlKind kind) {
        return aggregate.getAggCallList().stream().filter(call -> call.getAggregation().getKind() == kind).toList();
    }
}
