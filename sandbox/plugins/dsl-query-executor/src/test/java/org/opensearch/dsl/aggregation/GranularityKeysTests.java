/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class GranularityKeysTests extends OpenSearchTestCase {

    public void testKeyIncludesAggNameAtEachLevel() {
        List<GroupingInfo> path = List.of(new FieldGrouping("a", List.of("brand")), new FieldGrouping("b", List.of("category")));
        String key = GranularityKeys.granularityKey(path);

        assertTrue(key, key.contains("1#a"));
        assertTrue(key, key.contains("1#b"));
        // Renaming either level's aggregation changes the key: the name is identity at EVERY level.
        assertNotEquals(
            key,
            GranularityKeys.granularityKey(List.of(new FieldGrouping("z", List.of("brand")), new FieldGrouping("b", List.of("category"))))
        );
        assertNotEquals(
            key,
            GranularityKeys.granularityKey(List.of(new FieldGrouping("a", List.of("brand")), new FieldGrouping("z", List.of("category"))))
        );
    }

    public void testSiblingsOnSameFieldGetDifferentKeys() {
        // {"a": terms(brand){avg}, "b": terms(brand){sum}} — two granularities, not one.
        // A field-only key collapsed these into a single plan and merged their metrics and orders.
        String a = GranularityKeys.granularityKey(List.of(new FieldGrouping("a", List.of("brand"))));
        String b = GranularityKeys.granularityKey(List.of(new FieldGrouping("b", List.of("brand"))));

        assertNotEquals(a, b);
    }

    public void testCommaInFieldNameIsOneFieldNotTwo() {
        // A mapping key is an arbitrary JSON string, so a field may literally be named "a,b".
        // A bare comma join inside the level's field list makes these two keys equal.
        String oneField = GranularityKeys.granularityKey(List.of(new FieldGrouping("g", List.of("a,b"))));
        String twoFields = GranularityKeys.granularityKey(List.of(new FieldGrouping("g", List.of("a", "b"))));

        assertNotEquals(oneField, twoFields);
    }

    public void testDelimiterInAggNameDoesNotCollideWithAnotherPath() {
        // VALID_AGG_NAME only excludes '[', ']' and '>', so ':', '|' and '#' are legal in a name.
        String hostile = GranularityKeys.granularityKey(List.of(new FieldGrouping("a:5#brand|1", List.of("brand"))));
        String twoLevels = GranularityKeys.granularityKey(
            List.of(new FieldGrouping("a", List.of("brand")), new FieldGrouping("1", List.of("brand")))
        );

        assertNotEquals(hostile, twoLevels);
    }

    public void testEachLevelIsOrdinalTagged() {
        // The level ordinal is part of the encoding the assembly side parses back.
        String key = GranularityKeys.granularityKey(
            List.of(new FieldGrouping("a", List.of("brand")), new FieldGrouping("b", List.of("category")))
        );

        assertTrue(key, key.startsWith("0:"));
        assertTrue(key, key.contains("|1:"));
    }

    public void testRootKeyIsEmpty() {
        assertEquals(GranularityKeys.ROOT, GranularityKeys.granularityKey(List.of()));
        assertEquals("", GranularityKeys.ROOT);
    }

    public void testKeyIsStableAcrossCalls() {
        List<GroupingInfo> path = List.of(
            new FieldGrouping("by_brand", List.of("brand")),
            new FieldGrouping("by_cat", List.of("category"))
        );

        assertEquals(GranularityKeys.granularityKey(path), GranularityKeys.granularityKey(path));
        // Equal-but-distinct inputs must also produce the same key — this is the map-key contract
        // AggregationTreeWalker relies on to merge metrics at the same granularity.
        assertEquals(
            GranularityKeys.granularityKey(path),
            GranularityKeys.granularityKey(
                List.of(new FieldGrouping("by_brand", List.of("brand")), new FieldGrouping("by_cat", List.of("category")))
            )
        );
    }

    // ---- Prefix / ancestry helpers (F2.0) ----

    public void testKeyIsPrefixStructuredAlongPath() {
        String parent = key(level("a", "brand"));
        String child = key(level("a", "brand"), level("b", "category"));

        assertTrue(GranularityKeys.isAncestorKey(parent, child));
        assertFalse(GranularityKeys.isAncestorKey(child, parent));
    }

    public void testIsAncestorKeyRejectsSibling() {
        String a = key(level("a", "brand"));
        String b = key(level("b", "brand"));

        assertFalse(GranularityKeys.isAncestorKey(a, b));
        assertFalse(GranularityKeys.isAncestorKey(b, a));
        // A granularity is not its own strict ancestor.
        assertFalse(GranularityKeys.isAncestorKey(a, a));
    }

    public void testDelimiterInAggNameCannotForgeAncestry() {
        // Without length framing, a level-0 aggregation named to look like "a|1:2#x" could pass itself
        // off as the prefix of an unrelated two-level key.
        String hostile = key(level("a|1:2#x", "brand"));
        String unrelated = key(level("a", "brand"), level("b", "category"));

        assertFalse(GranularityKeys.isAncestorKey(hostile, unrelated));
        assertFalse(GranularityKeys.isAncestorKey(unrelated, hostile));
    }

    public void testCommaBearingFieldNameKeysAreNotAncestors() {
        String oneField = key(level("g", "a,b"));
        String twoFields = key(new FieldGrouping("g", List.of("a", "b")));

        assertFalse(GranularityKeys.isAncestorKey(oneField, twoFields));
        assertFalse(GranularityKeys.isAncestorKey(twoFields, oneField));
    }

    public void testRootKeyIsAncestorOfEverything() {
        assertTrue(GranularityKeys.isAncestorKey(GranularityKeys.ROOT, key(level("a", "brand"))));
        assertTrue(GranularityKeys.isAncestorKey(GranularityKeys.ROOT, key(level("a", "brand"), level("b", "category"))));
        // ...but not of itself: the no-GROUP-BY level is everyone's ancestor except its own.
        assertFalse(GranularityKeys.isAncestorKey(GranularityKeys.ROOT, GranularityKeys.ROOT));
    }

    public void testDirectChildrenOfReturnsOnlyOneLevelDeeper() {
        String a = key(level("a", "brand"));
        String ab = key(level("a", "brand"), level("b", "category"));
        String abc = key(level("a", "brand"), level("b", "category"), level("c", "size"));
        List<String> all = List.of(GranularityKeys.ROOT, a, ab, abc);

        assertEquals(List.of(ab), GranularityKeys.directChildrenOf(a, all));
        assertEquals(List.of(a), GranularityKeys.directChildrenOf(GranularityKeys.ROOT, all));
        assertEquals(List.of(abc), GranularityKeys.directChildrenOf(ab, all));
        assertEquals(List.of(), GranularityKeys.directChildrenOf(abc, all));
    }

    public void testDirectChildrenOfIgnoresUnrelatedBranches() {
        String a = key(level("a", "brand"));
        String ab = key(level("a", "brand"), level("b", "category"));
        String x = key(level("x", "brand"));
        String xy = key(level("x", "brand"), level("y", "category"));

        assertEquals(List.of(ab), GranularityKeys.directChildrenOf(a, List.of(a, ab, x, xy)));
    }

    public void testPrefixReassemblyOfThreeLevelPath() {
        String l1 = key(level("a", "brand"));
        String l2 = key(level("a", "brand"), level("b", "category"));
        String l3 = key(level("a", "brand"), level("b", "category"), level("c", "size"));
        List<String> all = List.of(l3, l1, l2); // deliberately not in depth order

        // Walking directChildrenOf from ROOT reassembles the one chain the three keys encode.
        List<String> chain = new ArrayList<>();
        String current = GranularityKeys.ROOT;
        while (true) {
            List<String> children = GranularityKeys.directChildrenOf(current, all);
            if (children.isEmpty()) {
                break;
            }
            assertEquals("a nest has exactly one child per level", 1, children.size());
            current = children.get(0);
            chain.add(current);
        }
        assertEquals(List.of(l1, l2, l3), chain);
    }

    // ---- Parse side (SC-7's second direction) ----

    public void testGranularityKeyRoundTripIsStable() {
        List<GroupingInfo> path = List.of(level("a", "brand"), level("b", "category"), level("c", "size"));
        String key = GranularityKeys.granularityKey(path);

        assertEquals(key, GranularityKeys.granularityKey(asGroupings(GranularityKeys.parseGranularityKey(key))));
        assertLevelsMatch(path, GranularityKeys.parseGranularityKey(key));

        // The delimiter-hostile name has to round-trip too, or every bucket join can drift with it.
        List<GroupingInfo> hostile = List.of(level("a|1:2#x", "brand"), level("b", "cat,egory"));
        String hostileKey = GranularityKeys.granularityKey(hostile);
        assertEquals(hostileKey, GranularityKeys.granularityKey(asGroupings(GranularityKeys.parseGranularityKey(hostileKey))));
        assertLevelsMatch(hostile, GranularityKeys.parseGranularityKey(hostileKey));
    }

    public void testParseReturnsOrdinalAggNameAndFieldsPerLevel() {
        List<GranularityKeys.GranularityLevel> levels = GranularityKeys.parseGranularityKey(
            key(level("a", "brand"), level("b", "category"))
        );

        assertEquals(2, levels.size());
        assertEquals(0, levels.get(0).ordinal());
        assertEquals("a", levels.get(0).aggName());
        assertEquals(List.of("brand"), levels.get(0).fieldNames());
        assertEquals(1, levels.get(1).ordinal());
        assertEquals("b", levels.get(1).aggName());
        assertEquals(List.of("category"), levels.get(1).fieldNames());
    }

    public void testParseOfRootKeyIsEmptyLevelList() {
        assertEquals(List.of(), GranularityKeys.parseGranularityKey(GranularityKeys.ROOT));
    }

    public void testParsedFieldNamesConcatenateToGroupByFieldNames() throws ConversionException {
        // This is what makes AggregationMetadata unnecessary on the assembly path: the parsed levels'
        // field names, concatenated in level order, ARE getGroupByFieldNames(). If this ever fails, the
        // assembler's column resolution is unsound.
        List<GroupingInfo> path = List.of(new FieldGrouping("a", List.of("brand", "name")), level("b", "status"));

        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        for (GroupingInfo grouping : path) {
            builder.addGrouping(grouping);
        }
        builder.requestImplicitCount();
        ConversionContext ctx = TestUtils.createContext();
        List<String> fromMetadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory()).getGroupByFieldNames();

        List<String> fromKey = GranularityKeys.parseGranularityKey(GranularityKeys.granularityKey(path))
            .stream()
            .flatMap(level -> level.fieldNames().stream())
            .toList();

        assertEquals(fromMetadata, fromKey);
    }

    public void testParseRejectsMalformedKey() {
        String valid = key(level("a", "brand"), level("b", "category"));

        // Truncated mid-segment.
        expectThrows(IllegalArgumentException.class, () -> GranularityKeys.parseGranularityKey(valid.substring(0, valid.length() - 3)));
        // Length prefix that overruns the key.
        expectThrows(IllegalArgumentException.class, () -> GranularityKeys.parseGranularityKey("0:99#a:7#5#brand"));
        // Trailing level separator: a level that was announced and never encoded.
        expectThrows(IllegalArgumentException.class, () -> GranularityKeys.parseGranularityKey(valid + "|"));
        // Missing length prefix.
        expectThrows(IllegalArgumentException.class, () -> GranularityKeys.parseGranularityKey("0:a:7#5#brand"));
        // Out-of-order level ordinal.
        expectThrows(IllegalArgumentException.class, () -> GranularityKeys.parseGranularityKey("1:1#a:7#5#brand"));
    }

    public void testParseDoesNotSplitOnDelimitersInsideNames() {
        List<GranularityKeys.GranularityLevel> levels = GranularityKeys.parseGranularityKey(key(level("x|1:y", "brand")));

        assertEquals(1, levels.size());
        assertEquals("x|1:y", levels.get(0).aggName());
    }

    public void testParseOfCommaBearingFieldNameYieldsOneFieldName() {
        String key = key(level("g", "a,b"));
        List<GranularityKeys.GranularityLevel> levels = GranularityKeys.parseGranularityKey(key);

        assertEquals(1, levels.size());
        assertEquals(List.of("a,b"), levels.get(0).fieldNames());
        assertEquals(key, GranularityKeys.granularityKey(asGroupings(levels)));
    }

    // ---- Helpers ----

    private static FieldGrouping level(String aggName, String... fieldNames) {
        return new FieldGrouping(aggName, List.of(fieldNames));
    }

    private static String key(GroupingInfo... levels) {
        return GranularityKeys.granularityKey(List.of(levels));
    }

    private static List<GroupingInfo> asGroupings(List<GranularityKeys.GranularityLevel> levels) {
        return levels.stream().<GroupingInfo>map(l -> new FieldGrouping(l.aggName(), l.fieldNames())).toList();
    }

    private static void assertLevelsMatch(List<GroupingInfo> expected, List<GranularityKeys.GranularityLevel> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(i, actual.get(i).ordinal());
            assertEquals(expected.get(i).getAggName(), actual.get(i).aggName());
            assertEquals(expected.get(i).getFieldNames(), actual.get(i).fieldNames());
        }
    }
}
