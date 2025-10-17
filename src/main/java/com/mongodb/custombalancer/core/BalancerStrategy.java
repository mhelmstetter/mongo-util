package com.mongodb.custombalancer.core;

import java.util.List;

/**
 * Strategy interface for different balancing approaches.
 * Implementations include: ActivityBasedStrategy, DataSizeBalancerStrategy,
 * CompositeBalancerStrategy, WhaleDetectionStrategy.
 */
public interface BalancerStrategy {

    /**
     * @return The name of this strategy (e.g., "activity", "dataSize", "whale")
     */
    String getName();

    /**
     * Determine if this strategy should run given the current context.
     * For example, whale detection might only run if whales are detected.
     *
     * @param context Current balancer state and metrics
     * @return true if strategy should run
     */
    boolean shouldRun(BalancerContext context);

    /**
     * Select prefix migrations to execute.
     * Should return migrations in priority order (most important first).
     *
     * @param context Current balancer state and metrics
     * @return List of migrations to execute, limited by maxMigrationsPerRound
     */
    List<PrefixMigration> selectMigrations(BalancerContext context);

    /**
     * Optional: Called before each balancing round to allow strategies
     * to refresh their internal state or metrics.
     *
     * @param context Current balancer state and metrics
     */
    default void prepareRound(BalancerContext context) {
        // Default: no-op
    }
}