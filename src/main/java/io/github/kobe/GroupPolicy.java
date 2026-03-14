package io.github.kobe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Defines how max concurrency and queue capacity are chosen for each groupKey.
 *
 * <p>Concurrency priority: per-group override &gt; resolver &gt; default value.</p>
 * <p>Queue threshold priority: per-group override &gt; default value.</p>
 *
 * <p>Note: {@code defaultMaxInFlightPerGroup}, {@code perGroupMaxInFlight}, and
 * {@code resolveMaxInFlight()} have been removed. In-flight limiting is now implicit:
 * tasks are either executing (holding a semaphore permit) or queued in the
 * {@code LinkedBlockingQueue} whose capacity is controlled by {@code queueThreshold}.</p>
 */
public final class GroupPolicy {

    private final Map<String, Integer> perGroupMaxConcurrency;
    private final ToIntFunction<String> concurrencyResolver;
    private final int defaultMaxConcurrencyPerGroup;

    private final int globalMaxInFlight;
    private final TaskLifecycleListener taskLifecycleListener;

    private final int defaultQueueThresholdPerGroup;
    private final Map<String, Integer> perGroupQueueThreshold;
    private final RejectionPolicy rejectionPolicy;
    private final RejectionHandler rejectionHandler;

    private GroupPolicy(Builder builder) {
        this.perGroupMaxConcurrency = builder.perGroupMaxConcurrency == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(builder.perGroupMaxConcurrency));
        this.concurrencyResolver = builder.concurrencyResolver;
        this.defaultMaxConcurrencyPerGroup = sanitize(builder.defaultMaxConcurrencyPerGroup);
        this.globalMaxInFlight = sanitize(builder.globalMaxInFlight);
        this.taskLifecycleListener = builder.taskLifecycleListener;
        this.defaultQueueThresholdPerGroup = sanitizeQueue(builder.defaultQueueThresholdPerGroup);
        this.perGroupQueueThreshold = builder.perGroupQueueThreshold == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(builder.perGroupQueueThreshold));
        this.rejectionPolicy = builder.rejectionPolicy;
        this.rejectionHandler = builder.rejectionHandler;
    }

    /**
     * Resolve the max concurrency for the given group key following the configured priority.
     *
     * @param groupKey group identifier, must not be null
     * @return max concurrency &gt;= 1
     */
    public int resolveConcurrency(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        Integer overridden = perGroupMaxConcurrency.get(groupKey);
        if (overridden != null) {
            return sanitize(overridden);
        }
        if (concurrencyResolver != null) {
            try {
                int resolved = concurrencyResolver.applyAsInt(groupKey);
                return sanitize(resolved);
            } catch (RuntimeException ex) {
                // fall through to default
            }
        }
        return defaultMaxConcurrencyPerGroup;
    }

    public int globalMaxInFlight() {
        return globalMaxInFlight;
    }

    /**
     * Resolve the queue threshold (LinkedBlockingQueue capacity) for the given group key.
     * Priority: per-group override &gt; default value.
     *
     * @param groupKey group identifier, must not be null
     * @return queue capacity &gt;= 0; {@link Integer#MAX_VALUE} means unbounded
     */
    public int resolveQueueThreshold(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        Integer overridden = perGroupQueueThreshold.get(groupKey);
        if (overridden != null) {
            return sanitizeQueue(overridden);
        }
        return defaultQueueThresholdPerGroup;
    }

    private int sanitize(int raw) {
        return raw >= 1 ? raw : 1;
    }

    private int sanitizeQueue(int raw) {
        return raw >= 0 ? raw : 0;
    }

    public Map<String, Integer> perGroupMaxConcurrency() {
        return perGroupMaxConcurrency;
    }

    public ToIntFunction<String> concurrencyResolver() {
        return concurrencyResolver;
    }

    public int defaultMaxConcurrencyPerGroup() {
        return defaultMaxConcurrencyPerGroup;
    }

    public TaskLifecycleListener taskLifecycleListener() {
        return taskLifecycleListener;
    }

    public int defaultQueueThresholdPerGroup() {
        return defaultQueueThresholdPerGroup;
    }

    public Map<String, Integer> perGroupQueueThreshold() {
        return perGroupQueueThreshold;
    }

    public RejectionPolicy rejectionPolicy() {
        return rejectionPolicy;
    }

    public RejectionHandler rejectionHandler() {
        return rejectionHandler;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Map<String, Integer> perGroupMaxConcurrency;
        private ToIntFunction<String> concurrencyResolver;
        private int defaultMaxConcurrencyPerGroup = 1;
        private int globalMaxInFlight = Integer.MAX_VALUE;
        private TaskLifecycleListener taskLifecycleListener;
        private int defaultQueueThresholdPerGroup = Integer.MAX_VALUE;
        private Map<String, Integer> perGroupQueueThreshold;
        private RejectionPolicy rejectionPolicy = RejectionPolicy.ABORT;
        private RejectionHandler rejectionHandler;

        private Builder() {
        }

        /**
         * Set per-group concurrency overrides (copied defensively).
         */
        public Builder perGroupMaxConcurrency(Map<String, Integer> perGroupMaxConcurrency) {
            this.perGroupMaxConcurrency = perGroupMaxConcurrency == null
                    ? null : new HashMap<>(perGroupMaxConcurrency);
            return this;
        }

        /**
         * Set resolver to compute concurrency dynamically.
         */
        public Builder concurrencyResolver(ToIntFunction<String> concurrencyResolver) {
            this.concurrencyResolver = concurrencyResolver;
            return this;
        }

        /**
         * Set default max concurrency per group (values &lt;1 will be coerced to 1).
         */
        public Builder defaultMaxConcurrencyPerGroup(int defaultMaxConcurrencyPerGroup) {
            this.defaultMaxConcurrencyPerGroup = defaultMaxConcurrencyPerGroup;
            return this;
        }

        /**
         * Set global max in-flight tasks across all groups.
         * Tasks beyond this limit are rejected immediately (not queued).
         */
        public Builder globalMaxInFlight(int globalMaxInFlight) {
            this.globalMaxInFlight = globalMaxInFlight;
            return this;
        }

        /**
         * Set a lifecycle listener for task events.
         */
        public Builder taskLifecycleListener(TaskLifecycleListener taskLifecycleListener) {
            this.taskLifecycleListener = taskLifecycleListener;
            return this;
        }

        /**
         * Set default queue capacity per group (the LinkedBlockingQueue capacity).
         * Tasks that cannot be dispatched and cannot fit in the queue are rejected.
         */
        public Builder defaultQueueThresholdPerGroup(int defaultQueueThresholdPerGroup) {
            this.defaultQueueThresholdPerGroup = defaultQueueThresholdPerGroup;
            return this;
        }

        /**
         * Set per-group queue capacity overrides (copied defensively).
         */
        public Builder perGroupQueueThreshold(Map<String, Integer> perGroupQueueThreshold) {
            this.perGroupQueueThreshold = perGroupQueueThreshold == null
                    ? null : new HashMap<>(perGroupQueueThreshold);
            return this;
        }

        /**
         * Set the built-in rejection policy.
         */
        public Builder rejectionPolicy(RejectionPolicy rejectionPolicy) {
            this.rejectionPolicy = rejectionPolicy;
            return this;
        }

        /**
         * Set a custom rejection handler. When configured, takes priority over {@link RejectionPolicy}.
         */
        public Builder rejectionHandler(RejectionHandler rejectionHandler) {
            this.rejectionHandler = rejectionHandler;
            return this;
        }

        public GroupPolicy build() {
            if (rejectionPolicy == null) {
                throw new IllegalArgumentException("rejectionPolicy must not be null");
            }
            if (defaultMaxConcurrencyPerGroup < 1) {
                throw new IllegalArgumentException(
                        "defaultMaxConcurrencyPerGroup must be >= 1, got: " + defaultMaxConcurrencyPerGroup);
            }
            if (globalMaxInFlight < 1) {
                throw new IllegalArgumentException(
                        "globalMaxInFlight must be >= 1, got: " + globalMaxInFlight);
            }
            if (defaultQueueThresholdPerGroup < 0) {
                throw new IllegalArgumentException(
                        "defaultQueueThresholdPerGroup must be >= 0, got: " + defaultQueueThresholdPerGroup);
            }
            if (perGroupMaxConcurrency != null) {
                perGroupMaxConcurrency.forEach((k, v) -> {
                    if (v == null || v < 1)
                        throw new IllegalArgumentException(
                                "perGroupMaxConcurrency value must be >= 1 for key: " + k);
                });
            }
            if (perGroupQueueThreshold != null) {
                perGroupQueueThreshold.forEach((k, v) -> {
                    if (v == null || v < 0)
                        throw new IllegalArgumentException(
                                "perGroupQueueThreshold value must be >= 0 for key: " + k);
                });
            }
            return new GroupPolicy(this);
        }
    }
}
