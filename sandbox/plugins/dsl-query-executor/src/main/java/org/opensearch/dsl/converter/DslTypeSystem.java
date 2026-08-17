/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The type system every DSL-emitted plan is built with. It differs from
 * {@link RelDataTypeSystem#DEFAULT} in exactly one place: {@code SUM}'s derived type is the width the
 * execution engine's accumulator actually produces, not the width of the summed column.
 *
 * <p><b>Why this exists.</b> Calcite's default {@code deriveSumType} returns the argument type
 * unchanged, so {@code SUM} over an {@code integer}-mapped field is declared {@code INTEGER} (i32).
 * The DataFusion backend accumulates every signed-integer sum in {@code Int64} and every
 * floating-point sum in {@code Float64}, so the plan and the engine disagree about that column's type.
 * On a single-shard index nobody cross-checks, and the query succeeds. On two or more shards
 * {@code OpenSearchAggregateSplitRule} splits the aggregate into PARTIAL/FINAL, the PARTIAL's output
 * crosses an exchange, and the coordinator registers that exchange input with the schema DataFusion
 * derived by lowering the PARTIAL ({@code Int64}) while the FINAL's Substrait declares Calcite's view
 * ({@code Int32}). The consumer rejects the read with
 * {@code Field '$f2' in Substrait schema has a different type (Int32) than the corresponding field in
 * the table schema (Int64)}, surfacing as {@code Failed to create exchange sink for stageId=1} and a
 * 500 on {@code _search}.
 *
 * <p>This is the same i32-vs-i64 disagreement, and the same 1-shard-hides-it/2-shards-fails-it
 * signature, that {@code IntegerReturnWideningCastAdapter} reconciles for scalar functions such as
 * {@code ARRAY_LENGTH}; aggregates need it too, and an aggregate call cannot carry its own cast, so the
 * reconciliation belongs in the type the plan declares.
 *
 * <p><b>Why the type system and not the metric translator.</b> The mismatching {@code SUM} is not
 * written by this plugin. A DSL {@code avg} aggregation is emitted as Calcite {@code AVG}, and the
 * engine's {@code OpenSearchAggregateReduceRule} (whose {@code FUNCTIONS_TO_REDUCE} contains
 * {@code AVG}, {@code STDDEV_*} and {@code VAR_*}) rewrites it into a rule-generated
 * {@code SUM}/{@code COUNT} pair — the unnamed calls Calcite renders as {@code $f2}/{@code $f3}. Those
 * calls infer their own return type through {@code ReturnTypes.AGG_SUM}, i.e. through
 * {@code cluster.getTypeFactory().getTypeSystem().deriveSumType(...)}, and the cluster is the one
 * {@code SearchSourceConverter.newBase} built for the plan. Fixing the declared type of the aggregation
 * the DSL <em>does</em> write would not touch them; fixing the type system does.
 *
 * <p>Nothing else is overridden: field types stay exactly as
 * {@code OpenSearchSchemaBuilder.mapFieldType} declared them, and any type family without a known
 * accumulator widening (decimal, interval, non-numeric) falls through to Calcite's default rather than
 * being guessed at.
 */
final class DslTypeSystem extends RelDataTypeSystemImpl {

    /** Stateless, so one instance serves every request. */
    static final RelDataTypeSystem INSTANCE = new DslTypeSystem();

    private DslTypeSystem() {}

    /**
     * Widens {@code SUM}'s type to the engine's accumulator width: signed integers to {@code BIGINT}
     * (i64), approximate numerics to {@code DOUBLE} (f64). Nullability is carried over from the
     * argument — dropping it would make {@code SUM} over a nullable column non-nullable and break the
     * empty-group contract Calcite relies on.
     *
     * @param typeFactory the plan's type factory
     * @param argumentType the summed column's type
     * @return the widened sum type, or Calcite's default for a family with no known widening
     */
    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        SqlTypeName widened = switch (argumentType.getSqlTypeName()) {
            case TINYINT, SMALLINT, INTEGER, BIGINT -> SqlTypeName.BIGINT;
            case REAL, FLOAT, DOUBLE -> SqlTypeName.DOUBLE;
            default -> null;
        };
        if (widened == null) {
            return super.deriveSumType(typeFactory, argumentType);
        }
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(widened), argumentType.isNullable());
    }
}
