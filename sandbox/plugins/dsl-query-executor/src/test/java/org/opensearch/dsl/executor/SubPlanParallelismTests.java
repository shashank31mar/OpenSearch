/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.OptionalInt;

/**
 * The {@code K_eff} formula, pinned cell by cell.
 *
 * <p>Every input is injected, so these tests need no node, no settings registry and no thread pool —
 * which is the whole reason the formula lives in a class of its own.
 */
public class SubPlanParallelismTests extends OpenSearchTestCase {

    /** The DataFusion backend's shipped multiplier default. */
    private static final double MULTIPLIER = 1.5;

    /** The engine's shipped per-node in-flight shard-request cap. */
    private static final int SHARD_REQUEST_CAP = 5;

    /**
     * One row of the design's gain grid: the inputs, the width they must produce, and the speedup the
     * gain model predicts for that cell — carried in the source so the join between formula and model is
     * visible to whoever edits either.
     *
     * @param vCpu coordinator vCPU count
     * @param expectedA the fragment count the gate admits, pinned so an edit to the A term is visible
     * @param poolSize the SEARCH pool size for that vCPU count, written as a literal
     * @param shardsOnBusiestNode S_node
     * @param expectedKEff the width the formula must produce
     * @param modelledSpeedup the gain model's K=2 column for this cell
     */
    private record GridCell(int vCpu, int expectedA, int poolSize, int shardsOnBusiestNode, int expectedKEff, String modelledSpeedup) {
    }

    /**
     * The 4 vCPU counts x 5 shard placements of the design's gain grid, at {@code K_setting = 2} and
     * {@code n = 3}.
     *
     * <p><b>The purpose is not to check arithmetic.</b> It is to make the formula and the gain model fail
     * <i>together</i> if either is edited alone: the rule the table encodes is {@code K_eff == 1} exactly
     * where the model reads {@code 1.00x}, and {@code K_eff == 2} wherever it reads {@code 1.20x} or
     * {@code 1.50x}. Only the three 8-vCPU cells at S &gt;= 4 are 1 — that is the cell where a fan-out
     * would buy {@code 2 x F} SEARCH threads and twice the memory for zero modelled gain, and it is
     * precisely the cell a {@code K_gate = A} simplification (or a {@code floor} in place of the
     * {@code ceil}) gets wrong.
     *
     * <p>Pool sizes are literals, not {@code (vCpu * 3 / 2) + 1} evaluated by the test: a formula here
     * would keep passing if that same formula were reintroduced as a production fallback.
     */
    private static final List<GridCell> GRID = List.of(
        // 8 vCPU: A = floor(8 * 1.5 / 4) = 3
        new GridCell(8, 3, 13, 1, 2, "1.50"),
        new GridCell(8, 3, 13, 2, 2, "1.50"),
        new GridCell(8, 3, 13, 4, 1, "1.00"),
        new GridCell(8, 3, 13, 8, 1, "1.00"),
        new GridCell(8, 3, 13, 16, 1, "1.00"),
        // 16 vCPU: A = 6
        new GridCell(16, 6, 25, 1, 2, "1.50"),
        new GridCell(16, 6, 25, 2, 2, "1.50"),
        new GridCell(16, 6, 25, 4, 2, "1.50"),
        new GridCell(16, 6, 25, 8, 2, "1.20"),
        new GridCell(16, 6, 25, 16, 2, "1.20"),
        // 32 vCPU: A = 12
        new GridCell(32, 12, 49, 1, 2, "1.50"),
        new GridCell(32, 12, 49, 2, 2, "1.50"),
        new GridCell(32, 12, 49, 4, 2, "1.50"),
        new GridCell(32, 12, 49, 8, 2, "1.50"),
        new GridCell(32, 12, 49, 16, 2, "1.50"),
        // 64 vCPU: A = 24
        new GridCell(64, 24, 97, 1, 2, "1.50"),
        new GridCell(64, 24, 97, 2, 2, "1.50"),
        new GridCell(64, 24, 97, 4, 2, "1.50"),
        new GridCell(64, 24, 97, 8, 2, "1.50"),
        new GridCell(64, 24, 97, 16, 2, "1.50")
    );

    public void testKEffGridMatchesGainModel() {
        for (GridCell cell : GRID) {
            SubPlanParallelism.Inputs in = gridInputs(cell);
            SubPlanParallelism.Decision decision = SubPlanParallelism.decide(in);
            String where = cell.vCpu() + " vCPU / S=" + cell.shardsOnBusiestNode() + " (model " + cell.modelledSpeedup() + "x)";
            assertEquals("A at " + where, cell.expectedA(), decision.a());
            assertEquals("K_eff at " + where, cell.expectedKEff(), decision.kEff());
            assertEquals("computeKEff must agree with decide at " + where, cell.expectedKEff(), SubPlanParallelism.computeKEff(in));
        }
        assertEquals("every cell of the 4x5 grid needs an assertion", 20, GRID.size());
    }

    /** Every 1.00x cell — and only those — is a K_eff of 1. Fails if ceil(A/F) is turned back into A. */
    public void testGridOnesAreExactlyTheNoGainCells() {
        for (GridCell cell : GRID) {
            boolean noModelledGain = "1.00".equals(cell.modelledSpeedup());
            int kEff = SubPlanParallelism.computeKEff(gridInputs(cell));
            assertEquals(
                "cell " + cell.vCpu() + " vCPU / S=" + cell.shardsOnBusiestNode() + " must be 1 iff the model says 1.00x",
                noModelledGain,
                kEff == 1
            );
        }
    }

    // ── The bounds ─────────────────────────────────────────────────────────

    public void testKEffClampsToSettingAndPlanCount() {
        // n = 2 leaves exactly one plan to fan out, so the width cannot exceed 1 whatever K_setting says.
        assertEquals(1, SubPlanParallelism.computeKEff(inputs(2, 2, 64, true, 1, OptionalInt.of(97))));
        // K_setting = 1 is the shipped default and binds on any machine.
        assertEquals(1, SubPlanParallelism.computeKEff(inputs(3, 1, 64, true, 1, OptionalInt.of(97))));
        // A single-plan query never reaches a fan-out decision; clamp(..., 1, 0) would be malformed.
        assertEquals(1, SubPlanParallelism.computeKEff(inputs(1, 2, 64, true, 1, OptionalInt.of(97))));
        assertEquals(1, SubPlanParallelism.computeKEff(inputs(0, 2, 64, true, 1, OptionalInt.of(97))));
    }

    /** The operator cap is re-applied here, not merely trusted from the setting. */
    public void testKSettingIsReclampedToTheHardMaximum() {
        // n = 9 so neither n - 1 nor any other term can be what limits the result to 2.
        assertEquals(SubPlanParallelism.MAX_K_SETTING, SubPlanParallelism.computeKEff(inputs(9, 7, 64, true, 1, OptionalInt.of(97))));
        assertEquals(1, SubPlanParallelism.computeKEff(inputs(9, 0, 64, true, 1, OptionalInt.of(97))));
    }

    /**
     * FIX 0, directly: {@code K_gate} counts sub-queries, so it is {@code ceil(A / F)}. At 8 vCPU the gate
     * admits 3 fragments; 2 shards on the busiest node makes a sub-query cost 2 of them (fits twice), 4
     * shards makes it cost 4 (fits once).
     */
    public void testKGateIsCeilOfFragmentsOverF() {
        SubPlanParallelism.Decision twoShards = SubPlanParallelism.decide(inputs(3, 2, 8, true, 2, OptionalInt.of(13)));
        assertEquals(3, twoShards.a());
        assertEquals(2, twoShards.f());
        assertEquals(OptionalInt.of(2), twoShards.kGate());
        assertEquals(2, twoShards.kEff());

        SubPlanParallelism.Decision fourShards = SubPlanParallelism.decide(inputs(3, 2, 8, true, 4, OptionalInt.of(13)));
        assertEquals(3, fourShards.a());
        assertEquals(4, fourShards.f());
        assertEquals("ceil(3/4) is 1 — a floor would agree here, but see the 2-shard cell", OptionalInt.of(1), fourShards.kGate());
        assertEquals(1, fourShards.kEff());
    }

    /** A floor would pin the 8 vCPU / 2 shard cell — the one the model predicts 1.50x for — at 1. */
    public void testCeilRatherThanFloorIsWhatKeepsTheGainCellOpen() {
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(inputs(3, 2, 8, true, 2, OptionalInt.of(13)));
        int floorWouldBe = Math.max(1, decision.a() / decision.f());
        assertEquals("floor(3/2) is 1, so a floor here would close the 1.50x cell", 1, floorWouldBe);
        assertEquals(2, decision.kGate().getAsInt());
    }

    // ── The two droppable terms ────────────────────────────────────────────

    /**
     * With no gated backend installed the gate term LEAVES the {@code min}. The 8 vCPU / 8 shard cell is
     * the discriminator: with the term present it is 1, so a 2 here can only mean the term was dropped
     * rather than clamped.
     */
    public void testKEffDropsGateTermWhenBackendAbsent() {
        SubPlanParallelism.Inputs present = inputs(3, 2, 8, true, 8, OptionalInt.of(13));
        assertEquals(1, SubPlanParallelism.computeKEff(present));

        SubPlanParallelism.Inputs absent = inputs(3, 2, 8, false, 8, OptionalInt.of(13));
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(absent);
        assertEquals("the gate term must be reported as dropped, not as 1", OptionalInt.empty(), decision.kGate());
        assertEquals(2, decision.kEff());
    }

    /** The multiplier is never read when the gate term is absent, so a poisoned placeholder is harmless. */
    public void testAbsentGateTermNeverReadsTheMultiplier() {
        SubPlanParallelism.Inputs poisoned = new SubPlanParallelism.Inputs(
            3,
            2,
            8,
            Double.NaN,
            4,
            false,
            8,
            SHARD_REQUEST_CAP,
            OptionalInt.of(13)
        );
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(poisoned);
        assertEquals(2, decision.kEff());
        assertEquals(OptionalInt.empty(), decision.kGate());
    }

    public void testKSearchBindsWhenPoolIsSmall() {
        // 32 vCPU puts K_gate at 3, so the pool term is the only thing that can produce a 1 here.
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(inputs(3, 2, 32, true, 8, OptionalInt.of(3)));
        assertEquals(5, decision.f());
        assertEquals(OptionalInt.of(3), decision.kGate());
        assertEquals(OptionalInt.of(1), decision.kSearch());
        assertEquals(1, decision.kEff());
    }

    /** The reserve is subtracted before the division, and the 10-thread cell is what proves it. */
    public void testSearchReserveIsSubtracted() {
        assertEquals(OptionalInt.of(1), SubPlanParallelism.decide(inputs(3, 2, 32, true, 8, OptionalInt.of(7))).kSearch());
        // floor((10 - 2) / 5) = 1. Without the reserve it would be floor(10 / 5) = 2, which is the whole
        // point of this cell: the assertion changes if SEARCH_RESERVE is dropped or zeroed.
        assertEquals(OptionalInt.of(1), SubPlanParallelism.decide(inputs(3, 2, 32, true, 8, OptionalInt.of(10))).kSearch());
        assertEquals(1, SubPlanParallelism.computeKEff(inputs(3, 2, 32, true, 8, OptionalInt.of(10))));
        // floor((12 - 2) / 5) = 2 — the first pool size at which the SEARCH term stops binding.
        assertEquals(OptionalInt.of(2), SubPlanParallelism.decide(inputs(3, 2, 32, true, 8, OptionalInt.of(12))).kSearch());
        assertEquals(2, SubPlanParallelism.computeKEff(inputs(3, 2, 32, true, 8, OptionalInt.of(12))));
    }

    /**
     * The anti-guess assertion, and the other half of
     * {@code DslQueryPlanExecutorTests#testSearchPoolSizeAbsentWhenExecutorIsNotOpenSearchThreadPoolExecutor}:
     * an unreadable pool size DROPS its term. Same inputs as {@link #testKSearchBindsWhenPoolIsSmall},
     * which yields 1 — so a 2 here proves the term was dropped rather than replaced by any default.
     */
    public void testKEffDropsSearchTermWhenPoolSizeAbsent() {
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(inputs(3, 2, 32, true, 8, OptionalInt.empty()));
        assertEquals(OptionalInt.empty(), decision.kSearch());
        assertEquals(2, decision.kEff());
    }

    public void testBothTermsDroppedLeavesSettingAndPlanCount() {
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(inputs(3, 2, 8, false, 16, OptionalInt.empty()));
        assertEquals(OptionalInt.empty(), decision.kGate());
        assertEquals(OptionalInt.empty(), decision.kSearch());
        assertEquals(2, decision.kEff());
    }

    // ── Degenerate inputs must not throw on the query path ─────────────────

    /** F is guarded at both ends, so a red index (0 shards found) cannot divide by zero. */
    public void testZeroShardsOnBusiestNodeIsTreatedAsOne() {
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(inputs(3, 2, 8, true, 0, OptionalInt.of(13)));
        assertEquals(1, decision.f());
        assertEquals(2, decision.kEff());
    }

    /** The producer clamps target_partitions, but this record is a seam — a 0 must not throw here. */
    public void testZeroTargetPartitionsDoesNotDivideByZero() {
        SubPlanParallelism.Inputs in = new SubPlanParallelism.Inputs(
            3,
            2,
            8,
            MULTIPLIER,
            0,
            true,
            1,
            SHARD_REQUEST_CAP,
            OptionalInt.of(13)
        );
        SubPlanParallelism.Decision decision = SubPlanParallelism.decide(in);
        assertEquals(12, decision.a());
        assertEquals(2, decision.kEff());
    }

    private static SubPlanParallelism.Inputs gridInputs(GridCell cell) {
        return new SubPlanParallelism.Inputs(
            3,
            2,
            cell.vCpu(),
            MULTIPLIER,
            4,
            true,
            cell.shardsOnBusiestNode(),
            SHARD_REQUEST_CAP,
            OptionalInt.of(cell.poolSize())
        );
    }

    /** The grid's fixed multiplier / target_partitions / cap, with the varying inputs as arguments. */
    private static SubPlanParallelism.Inputs inputs(
        int n,
        int kSetting,
        int vCpu,
        boolean gateTermPresent,
        int shardsOnBusiestNode,
        OptionalInt searchPoolSize
    ) {
        return new SubPlanParallelism.Inputs(
            n,
            kSetting,
            vCpu,
            MULTIPLIER,
            4,
            gateTermPresent,
            shardsOnBusiestNode,
            SHARD_REQUEST_CAP,
            searchPoolSize
        );
    }
}
