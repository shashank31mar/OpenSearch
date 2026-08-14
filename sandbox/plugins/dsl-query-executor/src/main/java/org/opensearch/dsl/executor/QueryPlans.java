/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One or more query plans produced by DSL to RelNode conversion.
 */
public final class QueryPlans {

    /** Identifies what part of the SearchResponse a plan populates. */
    public enum Type {
        /** Document hits. */
        HITS,
        /** Aggregation results. */
        AGGREGATION
    }

    /**
     * A single plan pairing a {@link Type} with a Calcite {@link RelNode}, plus the granularity it
     * populates.
     *
     * <p>A nested aggregation yields one plan per granularity level rather than one plan with child
     * plans: {@code granularity} says which level a plan populates, and a parent level's key is a
     * strict prefix of its children's, so nesting is recoverable from the keys alone.
     *
     * @param type what part of the response this plan produces
     * @param relNode the Calcite logical plan to execute
     * @param granularity the granularity key identifying which aggregation level this plan populates;
     *     {@code GranularityKeys.ROOT} for HITS plans and for a no-GROUP-BY aggregation
     */
    public record QueryPlan(Type type, RelNode relNode, String granularity) {
        /**
         * Creates a query plan.
         *
         * @param type what part of the response this plan produces
         * @param relNode the Calcite logical plan to execute
         * @param granularity the granularity key this plan populates, never null
         */
        public QueryPlan {
            Objects.requireNonNull(type, "type must not be null");
            Objects.requireNonNull(relNode, "relNode must not be null");
            Objects.requireNonNull(granularity, "granularity must not be null");
        }

        /** Returns what part of the response this plan produces. */
        @Override
        public Type type() {
            return type;
        }

        /** Returns the Calcite logical plan to execute. */
        @Override
        public RelNode relNode() {
            return relNode;
        }

        /** Returns the granularity key this plan populates. */
        @Override
        public String granularity() {
            return granularity;
        }
    }

    private final List<QueryPlan> plans;

    private QueryPlans(List<QueryPlan> plans) {
        this.plans = List.copyOf(plans);
    }

    /** Returns all plans. */
    public List<QueryPlan> getAll() {
        return plans;
    }

    /**
     * Returns all plans matching the given type.
     *
     * @param type the plan type to look up
     */
    public List<QueryPlan> get(Type type) {
        return plans.stream().filter(p -> p.type() == type).toList();
    }

    /**
     * Returns true if a plan with the given type exists.
     *
     * @param type the plan type to check
     */
    public boolean has(Type type) {
        return plans.stream().anyMatch(p -> p.type() == type);
    }

    /** Builder for constructing {@link QueryPlans}. */
    public static class Builder {
        private final List<QueryPlan> plans = new ArrayList<>();

        /** Creates a new empty builder. */
        public Builder() {}

        /**
         * Adds a plan.
         *
         * @param plan the plan to add
         */
        public Builder add(QueryPlan plan) {
            plans.add(plan);
            return this;
        }

        /** Builds the plans */
        public QueryPlans build() {
            return new QueryPlans(plans);
        }
    }
}
