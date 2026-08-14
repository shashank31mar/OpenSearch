/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;
import java.util.Objects;

/**
 * Field-based grouping: GROUP BY field1, field2, ...
 * Used by terms and multi_terms bucket aggregations.
 */
public class FieldGrouping implements GroupingInfo {

    private final String aggName;
    private final List<String> fieldNames;

    /**
     * Creates a field grouping.
     *
     * @param aggName the name of the bucket aggregation this grouping came from
     * @param fieldNames the field names to group by
     */
    public FieldGrouping(String aggName, List<String> fieldNames) {
        this.aggName = Objects.requireNonNull(aggName, "aggName must not be null");
        this.fieldNames = List.copyOf(fieldNames);
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public String getAggName() {
        return aggName;
    }
}
