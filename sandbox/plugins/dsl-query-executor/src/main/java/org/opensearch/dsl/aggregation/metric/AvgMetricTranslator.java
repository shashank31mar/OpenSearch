/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;

/** Translates AVG metric aggregation to Calcite. */
public class AvgMetricTranslator extends AbstractMetricTranslator<AvgAggregationBuilder> {

    /** Creates an AVG metric translator. */
    public AvgMetricTranslator() {}

    @Override
    public Class<AvgAggregationBuilder> getAggregationType() {
        return AvgAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.AVG;
    }

    @Override
    protected String getFieldName(AvgAggregationBuilder agg) {
        return agg.field();
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@link InternalAvg} stores {@code (sum, count)} and divides on read, while the plan already
     * divided and hands back one number. {@code (value, 1)} therefore renders the correct
     * {@code value} — and would be a lie under any further reduce, which merges sums and counts. That is
     * sound only because this response is coordinator-final: {@code SearchResponseBuilder} serialises it
     * straight to XContent and no reduce phase ever sees it. If the DSL path ever gains a reduce, the
     * engine has to return a real {@code (sum, count)} pair; it cannot be reconstructed from the implicit
     * {@code _count}, which counts documents while AVG divides by values.
     */
    @Override
    public InternalAggregation toInternalAggregation(String name, Object value) {
        Double avg = asDoubleOrNull(value);
        if (avg == null) {
            // AvgAggregator.buildEmptyAggregation: sum 0 / count 0 is NaN, which renders as null.
            return new InternalAvg(name, 0.0, 0L, DocValueFormat.RAW, null);
        }
        return new InternalAvg(name, avg, 1L, DocValueFormat.RAW, null);
    }
}
