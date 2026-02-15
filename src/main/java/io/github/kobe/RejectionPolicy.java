package io.github.kobe;

/**
 * Built-in rejection policies for when a task exceeds the queue threshold.
 */
public enum RejectionPolicy {

    /**
     * Throw a {@link RejectedTaskException}. This is the default policy.
     */
    ABORT,

    /**
     * Silently discard the task and return a {@link TaskStatus#REJECTED} result.
     */
    DISCARD,

    /**
     * Execute the task directly in the caller's virtual thread, bypassing concurrency permits.
     */
    CALLER_RUNS
}
