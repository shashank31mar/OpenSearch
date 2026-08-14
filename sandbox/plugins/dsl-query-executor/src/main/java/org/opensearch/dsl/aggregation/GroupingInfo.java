/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;

/**
 * Represents a grouping contribution from a bucket aggregation.
 * Implementations provide field-based grouping (terms) or
 * expression-based grouping (histogram, range) without modifying this interface.
 */
public interface GroupingInfo {

    /** Returns the logical field names this grouping contributes. */
    List<String> getFieldNames();

    /**
     * Returns the name of the bucket aggregation that contributed this grouping.
     * Part of the granularity identity: sibling aggregations over the same field are
     * different granularities, and sibling names are unique per level.
     */
    String getAggName();
}
