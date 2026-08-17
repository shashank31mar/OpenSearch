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
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

/** Translates MAX metric aggregation to Calcite. */
public class MaxMetricTranslator extends AbstractMetricTranslator<MaxAggregationBuilder> {

    /** Creates a MAX metric translator. */
    public MaxMetricTranslator() {}

    @Override
    public Class<MaxAggregationBuilder> getAggregationType() {
        return MaxAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.MAX;
    }

    @Override
    protected String getFieldName(MaxAggregationBuilder agg) {
        return agg.field();
    }

    @Override
    public InternalAggregation toInternalAggregation(String name, Object value) {
        Double max = asDoubleOrNull(value);
        // MaxAggregator.buildEmptyAggregation: NEGATIVE_INFINITY is the "no value" sentinel InternalMax
        // renders as null. Never 0 — 0 is a legitimate maximum.
        return new InternalMax(name, max == null ? Double.NEGATIVE_INFINITY : max, DocValueFormat.RAW, null);
    }
}
