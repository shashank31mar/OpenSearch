/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.ArrayList;
import java.util.Collection;
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

    /**
     * One grouping level exactly as the key encodes it.
     *
     * @param ordinal the level's position in the bucket path, outermost level being 0
     * @param aggName the name of the bucket aggregation at this level
     * @param fieldNames the fields this level groups by, in declaration order
     */
    public record GranularityLevel(int ordinal, String aggName, List<String> fieldNames) {
    }

    /**
     * Inverse of {@link #granularityKey(List)}: the ordered levels the key encodes.
     *
     * <p>This is what makes the key a lossless stand-in for the group-by half of
     * {@code AggregationMetadata} on the response-assembly path. Concatenating the levels'
     * {@link GranularityLevel#fieldNames()} in level order reproduces
     * {@code AggregationMetadata.getGroupByFieldNames()} exactly, because that list is built by the
     * same concatenation over the same {@code groupings} the key was built from — so assembly never
     * needs the metadata object, which it cannot reach anyway.
     *
     * <p>The parse walks the {@code <len>#} prefixes rather than splitting on delimiters: an
     * aggregation name may legally contain {@code '|'}, {@code ':'}, {@code ','} or {@code '#'}, so a
     * {@code String.split} parse would shift segment boundaries on a hostile name — the exact hole the
     * framing closes.
     *
     * @param key a key produced by {@link #granularityKey(List)}
     * @return the levels the key encodes; {@link #ROOT} yields an empty list
     * @throws IllegalArgumentException if the key does not decode — never a partial level list, because
     *     a partial parse silently mis-joins buckets
     */
    public static List<GranularityLevel> parseGranularityKey(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (ROOT.equals(key)) {
            return List.of();
        }

        List<GranularityLevel> levels = new ArrayList<>();
        Cursor cursor = new Cursor(key);
        while (true) {
            if (levels.isEmpty() == false) {
                cursor.expect(LEVEL_SEP);
            }
            int ordinal = cursor.readOrdinal();
            if (ordinal != levels.size()) {
                throw malformed(key, "level ordinal " + ordinal + " where " + levels.size() + " was expected");
            }
            String aggName = cursor.readFramed("aggregation name");
            cursor.expect(SEGMENT_SEP);
            List<String> fieldNames = parseFieldList(cursor.readFramed("field list"));
            levels.add(new GranularityLevel(ordinal, aggName, fieldNames));
            if (cursor.atEnd()) {
                return List.copyOf(levels);
            }
        }
    }

    /**
     * True iff {@code parent} is a strict ancestor granularity of {@code child}.
     *
     * <p>Ancestry is a prefix relation because {@link #granularityKey(List)} appends one framed level
     * per bucket path step: a parent's key is a strict prefix of every descendant's key, ending on a
     * level boundary. {@link #ROOT} — the no-GROUP-BY level — is an ancestor of everything.
     *
     * @param parent the candidate ancestor key
     * @param child the candidate descendant key
     * @return true if parent is a strict ancestor of child
     */
    public static boolean isAncestorKey(String parent, String child) {
        Objects.requireNonNull(parent, "parent must not be null");
        Objects.requireNonNull(child, "child must not be null");
        if (parent.equals(child)) {
            return false;
        }
        if (ROOT.equals(parent)) {
            return child.isEmpty() == false;
        }
        // The LEVEL_SEP check is what keeps a longer level from matching a shorter one's prefix: the
        // framing makes every level boundary unambiguous, so a name cannot fake one.
        return child.startsWith(parent) && child.charAt(parent.length()) == LEVEL_SEP;
    }

    /**
     * The keys in {@code all} that are exactly one grouping level below {@code parent}.
     *
     * <p>Assembly walks the granularity forest with this: a parent bucket's sub-buckets come from its
     * direct children, and a grandchild must not be attached directly to a grandparent.
     *
     * @param parent the parent granularity key ({@link #ROOT} for the top level)
     * @param all the granularity keys to select from
     * @return the direct children, in {@code all}'s iteration order
     * @throws IllegalArgumentException if any key does not decode
     */
    public static List<String> directChildrenOf(String parent, Collection<String> all) {
        Objects.requireNonNull(parent, "parent must not be null");
        Objects.requireNonNull(all, "all must not be null");
        int parentDepth = parseGranularityKey(parent).size();

        List<String> children = new ArrayList<>();
        for (String candidate : all) {
            if (isAncestorKey(parent, candidate) && parseGranularityKey(candidate).size() == parentDepth + 1) {
                children.add(candidate);
            }
        }
        return List.copyOf(children);
    }

    // Kept a total inverse of the build side on purpose: an empty field list encodes as "0#" and parses
    // back to an empty list rather than throwing, so parse never rejects a key the builder can emit.
    // A level with no grouping column is meaningless to assembly, but that is assembly's error to
    // raise against the plan it holds, not a decoding failure.
    private static List<String> parseFieldList(String fieldList) {
        Cursor cursor = new Cursor(fieldList);
        List<String> fieldNames = new ArrayList<>();
        while (cursor.atEnd() == false) {
            fieldNames.add(cursor.readFramed("field name"));
        }
        return List.copyOf(fieldNames);
    }

    private static IllegalArgumentException malformed(String key, String detail) {
        return new IllegalArgumentException("Malformed granularity key [" + key + "]: " + detail);
    }

    /** A read cursor over a key (or over one level's framed field list). */
    private static final class Cursor {

        private final String text;
        private int pos;

        Cursor(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return pos == text.length();
        }

        void expect(char expected) {
            if (pos >= text.length() || text.charAt(pos) != expected) {
                throw malformed(text, "expected '" + expected + "' at offset " + pos);
            }
            pos++;
        }

        int readOrdinal() {
            int value = readNumber("level ordinal");
            expect(SEGMENT_SEP);
            return value;
        }

        String readFramed(String what) {
            int length = readNumber(what + " length prefix");
            expect(LENGTH_SEP);
            if (length > text.length() - pos) {
                throw malformed(text, what + " length " + length + " overruns the key at offset " + pos);
            }
            String value = text.substring(pos, pos + length);
            pos += length;
            return value;
        }

        private int readNumber(String what) {
            int start = pos;
            while (pos < text.length() && text.charAt(pos) >= '0' && text.charAt(pos) <= '9') {
                pos++;
            }
            if (pos == start) {
                throw malformed(text, "missing " + what + " at offset " + start);
            }
            try {
                return Integer.parseInt(text, start, pos, 10);
            } catch (NumberFormatException e) {
                throw malformed(text, what + " at offset " + start + " is not a usable number");
            }
        }
    }
}
