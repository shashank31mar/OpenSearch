/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregationMetadataBuilderTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testResolvesFieldGroupingToCorrectIndex() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        // brand is the 3rd field (index 2) in TestUtils schema: name, price, brand, rating
        builder.addGrouping(new FieldGrouping("by_brand", List.of("brand")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(metadata.getGroupByBitSet().get(2));
        assertEquals(1, metadata.getGroupByBitSet().cardinality());
    }

    public void testResolvesMultipleFieldGroupings() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping("by_brand", List.of("brand")));
        builder.addGrouping(new FieldGrouping("by_name", List.of("name")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(metadata.getGroupByBitSet().get(0)); // name is index 0
        assertTrue(metadata.getGroupByBitSet().get(2)); // brand is index 2
        assertEquals(2, metadata.getGroupByBitSet().cardinality());
    }

    public void testThrowsForUnknownField() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping("by_missing", List.of("nonexistent")));
        builder.requestImplicitCount();

        expectThrows(ConversionException.class, () -> builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory()));
    }

    public void testRejectsAggregationNamedLikeTheImplicitCount() throws ConversionException {
        // "_count" is a legal aggregation name (VALID_AGG_NAME only excludes '[', ']' and '>') and it is the
        // implicit COUNT(*) column's name, so the response assembler — which resolves every column by name —
        // would read the user's metric as the bucket's doc_count. Refuse the request instead.
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping("by_brand", List.of("brand")));
        builder.addAggregateCall(sumOfPrice(), AggregationMetadataBuilder.IMPLICIT_COUNT_NAME);
        builder.requestImplicitCount();

        ConversionException e = expectThrows(
            ConversionException.class,
            () -> builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("reserved for a bucket's doc_count"));
    }

    public void testRejectsAggregationNamedLikeAGroupingField() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping("by_brand", List.of("brand")));
        builder.addAggregateCall(sumOfPrice(), "brand");
        builder.requestImplicitCount();

        ConversionException e = expectThrows(
            ConversionException.class,
            () -> builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("collides with group-by field"));
    }

    public void testAllowsTheSameFieldGroupedAtTwoNestedLevels() throws ConversionException {
        // Nesting two bucket aggregations on one field is legal DSL. The repeated grouping index collapses
        // to a single column and every reference to that name resolves to the same value, so this must NOT
        // be caught by the collision guard.
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping("outer", List.of("brand")));
        builder.addGrouping(new FieldGrouping("inner", List.of("brand")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(1, metadata.getGroupByBitSet().cardinality());
        assertEquals(List.of("brand", "brand"), metadata.getGroupByFieldNames());
    }

    public void testDistinctNamesStillBuild() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping("by_brand", List.of("brand")));
        builder.addAggregateCall(sumOfPrice(), "total_price");
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(List.of("total_price", AggregationMetadataBuilder.IMPLICIT_COUNT_NAME), metadata.getAggregateFieldNames());
    }

    /** SUM(price) — price is index 1 in the TestUtils schema. */
    private AggregateCall sumOfPrice() {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(1),
            -1,
            RelCollations.EMPTY,
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
            "sum_price"
        );
    }
}
