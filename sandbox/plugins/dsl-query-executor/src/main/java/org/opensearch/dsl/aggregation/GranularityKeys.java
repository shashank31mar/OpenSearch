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
 * Builds the key that identifies one aggregation granularity — the accumulated bucket path a set of
 * metrics sits under.
 *
 * <p>The key is the identity a {@code QueryPlan} carries out of conversion, so it has to distinguish
 * everything the DSL can distinguish. Grouping field names alone do not: in
 * {@code {"a": terms(brand){avg}, "b": terms(brand){sum}}} both siblings group by {@code brand}, so a
 * field-only key collapsed them into a single plan and merged their metrics and bucket orders. The
 * bucket aggregation's own name is therefore part of the key; sibling names are unique per level, so
 * the ordered name path is a valid identity. A <em>metric</em> name is never part of the key — metrics
 * at the same bucket path share one granularity and must stay in one plan.
 *
 * <p>Every segment is length-framed as {@code <len>#<value>} because names are almost unconstrained:
 * {@code AggregatorFactories.VALID_AGG_NAME} only excludes {@code [}, {@code ]} and {@code >}, and a
 * mapping key is an arbitrary JSON string, so {@code ':'}, {@code '|'}, {@code ','} and {@code '#'}
 * may all appear inside an aggregation or field name. Length framing makes the encoding injective — no
 * name can shift a segment boundary, so no name can forge another granularity's key or its ancestry —
 * and injective means the key stays decodable by the response-assembly side.
 */
public final class GranularityKeys {

    /** Separates one grouping level from the next. */
    private static final char LEVEL_SEP = '|';

    /** Separates the segments within one level. */
    private static final char SEGMENT_SEP = ':';

    /** Separates a segment's length prefix from its value. */
    private static final char LENGTH_SEP = '#';

    /** Root granularity (no GROUP BY): the empty key. */
    public static final String ROOT = "";

    private GranularityKeys() {}

    /**
     * Builds the granularity key for a bucket path.
     *
     * <p>Level {@code i} encodes as {@code <i>:<len>#<aggName>:<len>#<fieldList>}, where the field
     * list length-frames each field name in turn, and levels are joined by {@code '|'}. An empty path
     * encodes as {@link #ROOT}. Because levels are appended in path order, a parent granularity's key
     * is a strict prefix of every descendant's key, ending on a level boundary.
     *
     * @param groupings the accumulated bucket path, outermost level first
     * @return the granularity key; equal inputs always produce an equal key
     */
    public static String granularityKey(List<GroupingInfo> groupings) {
        Objects.requireNonNull(groupings, "groupings must not be null");
        if (groupings.isEmpty()) {
            return ROOT;
        }

        StringBuilder key = new StringBuilder();
        for (int i = 0; i < groupings.size(); i++) {
            if (i > 0) {
                key.append(LEVEL_SEP);
            }
            GroupingInfo grouping = groupings.get(i);

            // The field list is one framed segment whose contents are themselves framed per field: a
            // bare "field1,field2" join is not injective for a field name that contains ','.
            StringBuilder fieldList = new StringBuilder();
            for (String fieldName : grouping.getFieldNames()) {
                appendFramed(fieldList, fieldName, "field name");
            }

            key.append(i).append(SEGMENT_SEP);
            appendFramed(key, grouping.getAggName(), "aggregation name");
            key.append(SEGMENT_SEP);
            appendFramed(key, fieldList.toString(), "field list");
        }
        return key.toString();
    }

    private static void appendFramed(StringBuilder out, String value, String what) {
        Objects.requireNonNull(value, what + " must not be null");
        out.append(value.length()).append(LENGTH_SEP).append(value);
    }
}
