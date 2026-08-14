/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.test.OpenSearchTestCase;

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
}
