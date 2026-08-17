/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class MetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testAvgTranslator() throws ConversionException {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new AvgAggregationBuilder("avg_price").field("price"), ctx.getRowType());

        assertEquals(SqlKind.AVG, call.getAggregation().getKind());
        assertEquals("avg_price", call.getName());
        assertEquals(1, call.getArgList().size());
        assertEquals(1, call.getArgList().get(0).intValue()); // price is index 1
    }

    public void testSumTranslator() throws ConversionException {
        SumMetricTranslator translator = new SumMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new SumAggregationBuilder("total").field("price"), ctx.getRowType());

        assertEquals(SqlKind.SUM, call.getAggregation().getKind());
        assertEquals("total", call.getName());
    }

    public void testMinTranslator() throws ConversionException {
        MinMetricTranslator translator = new MinMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new MinAggregationBuilder("min_price").field("price"), ctx.getRowType());

        assertEquals(SqlKind.MIN, call.getAggregation().getKind());
        assertEquals("min_price", call.getName());
    }

    public void testMaxTranslator() throws ConversionException {
        MaxMetricTranslator translator = new MaxMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new MaxAggregationBuilder("max_price").field("price"), ctx.getRowType());

        assertEquals(SqlKind.MAX, call.getAggregation().getKind());
        assertEquals("max_price", call.getName());
    }

    public void testThrowsForUnknownField() {
        AvgMetricTranslator translator = new AvgMetricTranslator();

        expectThrows(
            ConversionException.class,
            () -> translator.toAggregateCall(new AvgAggregationBuilder("bad").field("nonexistent"), ctx.getRowType())
        );
    }

    public void testAggregateFieldName() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        assertEquals("avg_price", translator.getAggregateFieldName(new AvgAggregationBuilder("avg_price").field("price")));
    }

    // ---- Response leaves (F3.1) ----

    public void testAvgToInternalAggregation() {
        InternalAggregation agg = new AvgMetricTranslator().toInternalAggregation("avg_price", 850.0);

        assertThat(agg, instanceOf(InternalAvg.class));
        assertEquals("avg_price", agg.getName());
        // InternalAvg divides sum by count on read, so the plan's already-averaged value must come back
        // unchanged rather than halved or doubled.
        assertEquals(850.0, ((InternalAvg) agg).getValue(), 0.0);
    }

    public void testSumToInternalAggregation() {
        InternalAggregation agg = new SumMetricTranslator().toInternalAggregation("total", 2550L);

        assertThat(agg, instanceOf(InternalSum.class));
        assertEquals("total", agg.getName());
        assertEquals(2550.0, ((InternalSum) agg).getValue(), 0.0);
    }

    public void testMinToInternalAggregation() {
        InternalAggregation agg = new MinMetricTranslator().toInternalAggregation("min_price", 12);

        assertThat(agg, instanceOf(InternalMin.class));
        assertEquals("min_price", agg.getName());
        assertEquals(12.0, ((InternalMin) agg).getValue(), 0.0);
    }

    public void testMaxToInternalAggregation() {
        InternalAggregation agg = new MaxMetricTranslator().toInternalAggregation("max_price", 999);

        assertThat(agg, instanceOf(InternalMax.class));
        assertEquals("max_price", agg.getName());
        assertEquals(999.0, ((InternalMax) agg).getValue(), 0.0);
    }

    public void testIntegerInputValueIsWidened() {
        // Calcite pins an aggregate's return type to its argument's, so an INTEGER column yields Integer
        // even under AVG. The leaf must render it exactly as it renders a Double.
        InternalAggregation fromInteger = new AvgMetricTranslator().toInternalAggregation("avg_price", 850);
        InternalAggregation fromDouble = new AvgMetricTranslator().toInternalAggregation("avg_price", 850.0);

        assertEquals(((InternalAvg) fromDouble).getValue(), ((InternalAvg) fromInteger).getValue(), 0.0);
        assertEquals(850.0, ((InternalAvg) fromInteger).getValue(), 0.0);
    }

    public void testNullValueYieldsEmptyMetric() {
        // A no-GROUP-BY metric over an empty result set is legitimately null. Each metric maps it to its
        // OWN OpenSearch empty convention — never to 0, which is a legitimate value for all four.
        assertTrue(Double.isNaN(((InternalAvg) new AvgMetricTranslator().toInternalAggregation("a", null)).getValue()));
        assertEquals(0.0, ((InternalSum) new SumMetricTranslator().toInternalAggregation("s", null)).getValue(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, ((InternalMin) new MinMetricTranslator().toInternalAggregation("mn", null)).getValue(), 0.0);
        assertEquals(Double.NEGATIVE_INFINITY, ((InternalMax) new MaxMetricTranslator().toInternalAggregation("mx", null)).getValue(), 0.0);
    }

    public void testNonNumericValueFailsRatherThanFabricating() {
        // The plan cannot produce this shape; if it ever does, failing the request beats rendering a made
        // up number that every caller would believe.
        expectThrows(IllegalStateException.class, () -> new SumMetricTranslator().toInternalAggregation("s", "not-a-number"));
    }

    public void testEveryRegisteredMetricTranslatorImplementsToInternalAggregation() {
        // Catches a fifth metric registered without a response leaf: a query the converter accepts would
        // otherwise blow up only at response time.
        int checked = 0;
        for (AggregationTranslator<?> translator : AggregationRegistryFactory.create().all()) {
            if (translator instanceof MetricTranslator<?> metric) {
                InternalAggregation agg = metric.toInternalAggregation("m", 1.0);
                assertNotNull(translator.getClass().getSimpleName() + " returned no aggregation", agg);
                assertEquals("m", agg.getName());
                checked++;
            }
        }
        assertEquals("every registered metric translator must be exercised", 4, checked);
    }
}
