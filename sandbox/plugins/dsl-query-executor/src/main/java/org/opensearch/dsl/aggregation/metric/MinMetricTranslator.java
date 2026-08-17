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
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;

/** Translates MIN metric aggregation to Calcite. */
public class MinMetricTranslator extends AbstractMetricTranslator<MinAggregationBuilder> {

    /** Creates a MIN metric translator. */
    public MinMetricTranslator() {}

    @Override
    public Class<MinAggregationBuilder> getAggregationType() {
        return MinAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.MIN;
    }

    @Override
    protected String getFieldName(MinAggregationBuilder agg) {
        return agg.field();
    }

    @Override
    public InternalAggregation toInternalAggregation(String name, Object value) {
        Double min = asDoubleOrNull(value);
        // MinAggregator.buildEmptyAggregation: POSITIVE_INFINITY is the "no value" sentinel InternalMin
        // renders as null. Never 0 — 0 is a legitimate minimum.
        return new InternalMin(name, min == null ? Double.POSITIVE_INFINITY : min, DocValueFormat.RAW, null);
    }
}
