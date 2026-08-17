/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Shared test utilities for creating Calcite objects.
 * Mockito can't mock Calcite classes due to classloader conflicts with OpenSearch's
 * RandomizedRunner, so tests use real objects built here.
 *
 * Standard test schema: name (VARCHAR), price (INTEGER), brand (VARCHAR), rating (DOUBLE),
 * created_date (DATE), is_active (BOOLEAN), timestamp (BIGINT), location (GEOMETRY),
 * status (VARCHAR), binary_data (VARBINARY).
 */
public class TestUtils {

    private TestUtils() {}

    /** Creates a LogicalTableScan backed by the standard test schema. */
    public static LogicalTableScan createTestRelNode() {
        Infra infra = buildInfra();
        return LogicalTableScan.create(infra.cluster, infra.table, List.of());
    }

    /**
     * A {@link RelNode} whose row type is exactly the given columns — a stand-in for an executed plan on
     * the response-assembly path, where all that matters about a plan is the column names its rows carry.
     *
     * <p>Deliberately an {@link AbstractRelNode} rather than a {@code LogicalValues}: the Calcite factory
     * methods route through {@code cluster.getMetadataQuery()}, which is exactly the thing assembly must
     * never touch, so building the fixture must not depend on it either.
     *
     * @param columnNames the row type's field names, in order
     * @return a plan node reporting that row type
     */
    public static RelNode createRelNodeWithColumns(List<String> columnNames) {
        Infra infra = buildInfra();
        RelDataTypeFactory typeFactory = infra.cluster.getTypeFactory();
        RelDataTypeFactory.Builder rowType = typeFactory.builder();
        for (String columnName : columnNames) {
            rowType.add(columnName, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
        }
        return new FixedRowTypeRel(infra.cluster, rowType.build());
    }

    /** Creates a ConversionContext with the given search source and standard test schema. */
    public static ConversionContext createContext(SearchSourceBuilder searchSource) {
        Infra infra = buildInfra();
        return new ConversionContext(searchSource, infra.cluster, infra.table);
    }

    /** Creates a ConversionContext with an empty search source and standard test schema. */
    public static ConversionContext createContext() {
        return createContext(new SearchSourceBuilder());
    }

    /**
     * Drops the granularity keys from walker output, for tests that assert on metadata only.
     *
     * @param granularities the walker output
     * @return the metadata, in walker order
     */
    public static List<AggregationMetadata> metadataOf(List<AggregationTreeWalker.Granularity> granularities) {
        return granularities.stream().map(AggregationTreeWalker.Granularity::metadata).toList();
    }

    private static Infra buildInfra() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                // Nullable fields — matches OpenSearchSchemaBuilder behavior
                return tf.builder()
                    .add("name", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.INTEGER), true))
                    .add("brand", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("rating", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.DOUBLE), true))
                    .add("created_date", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.DATE), true))
                    .add("is_active", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BOOLEAN), true))
                    .add("timestamp", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true))
                    .add("location", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.GEOMETRY), true))
                    .add("status", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("binary_data", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARBINARY), true))
                    .build();
            }
        });

        CalciteCatalogReader reader = new CalciteCatalogReader(
            CalciteSchema.from(schema),
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );
        RelOptTable table = Objects.requireNonNull(reader.getTable(List.of("test")));
        return new Infra(cluster, table);
    }

    private record Infra(RelOptCluster cluster, RelOptTable table) {
    }

    /** A plan node that reports a fixed row type and nothing else. See {@link #createRelNodeWithColumns}. */
    private static final class FixedRowTypeRel extends AbstractRelNode {

        private final RelDataType fixedRowType;

        FixedRowTypeRel(RelOptCluster cluster, RelDataType fixedRowType) {
            super(cluster, cluster.traitSetOf(Convention.NONE));
            this.fixedRowType = fixedRowType;
        }

        @Override
        protected RelDataType deriveRowType() {
            return fixedRowType;
        }
    }
}
