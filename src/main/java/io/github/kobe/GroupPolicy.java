package io.github.kobe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Defines how max concurrency is chosen for each groupKey.
 * <p>Priority: per-group override &gt; resolver &gt; default value.</p>
 */
public final class GroupPolicy {

    private final Map<String, Integer> perGroupMaxConcurrency;
    private final ToIntFunction<String> concurrencyResolver;
    private final int defaultMaxConcurrencyPerGroup;

    private final int globalMaxInFlight;
    private final int defaultMaxInFlightPerGroup;
    private final Map<String, Integer> perGroupMaxInFlight;
    private final TaskLifecycleListener taskLifecycleListener;

    private GroupPolicy(Builder builder) {
        this.perGroupMaxConcurrency = builder.perGroupMaxConcurrency == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(builder.perGroupMaxConcurrency));
        this.concurrencyResolver = builder.concurrencyResolver;
        this.defaultMaxConcurrencyPerGroup = sanitize(builder.defaultMaxConcurrencyPerGroup);
        this.globalMaxInFlight = sanitize(builder.globalMaxInFlight);
        this.defaultMaxInFlightPerGroup = sanitize(builder.defaultMaxInFlightPerGroup);
        this.perGroupMaxInFlight = builder.perGroupMaxInFlight == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(builder.perGroupMaxInFlight));
        this.taskLifecycleListener = builder.taskLifecycleListener;
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

    /**
     * Resolve the max in-flight tasks for the given group key.
     * Priority: per-group override &gt; default value.
     *
     * @param groupKey group identifier, must not be null
     * @return max in-flight tasks &gt;= 1
     */
    public int resolveMaxInFlight(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        Integer overridden = perGroupMaxInFlight.get(groupKey);
        if (overridden != null) {
            return sanitize(overridden);
        }
        return defaultMaxInFlightPerGroup;
    }

    public int globalMaxInFlight() {
        return globalMaxInFlight;
    }

    private int sanitize(int raw) {
        return raw >= 1 ? raw : 1;
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

    public int defaultMaxInFlightPerGroup() {
        return defaultMaxInFlightPerGroup;
    }

    public Map<String, Integer> perGroupMaxInFlight() {
        return perGroupMaxInFlight;
    }

    public TaskLifecycleListener taskLifecycleListener() {
        return taskLifecycleListener;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Map<String, Integer> perGroupMaxConcurrency;
        private ToIntFunction<String> concurrencyResolver;
        private int defaultMaxConcurrencyPerGroup = 1;
        private int globalMaxInFlight = Integer.MAX_VALUE;
        private int defaultMaxInFlightPerGroup = Integer.MAX_VALUE;
        private Map<String, Integer> perGroupMaxInFlight;
        private TaskLifecycleListener taskLifecycleListener;

        private Builder() {
        }

        /**
         * Set per-group overrides (copied defensively).
         */
        public Builder perGroupMaxConcurrency(Map<String, Integer> perGroupMaxConcurrency) {
            this.perGroupMaxConcurrency = perGroupMaxConcurrency == null ? null : new HashMap<>(perGroupMaxConcurrency);
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
         */
        public Builder globalMaxInFlight(int globalMaxInFlight) {
            this.globalMaxInFlight = globalMaxInFlight;
            return this;
        }

        /**
         * Set default max in-flight tasks per group.
         */
        public Builder defaultMaxInFlightPerGroup(int defaultMaxInFlightPerGroup) {
            this.defaultMaxInFlightPerGroup = defaultMaxInFlightPerGroup;
            return this;
        }

        /**
         * Set per-group max in-flight overrides (copied defensively).
         */
        public Builder perGroupMaxInFlight(Map<String, Integer> perGroupMaxInFlight) {
            this.perGroupMaxInFlight = perGroupMaxInFlight == null ? null : new HashMap<>(perGroupMaxInFlight);
            return this;
        }

        /**
         * Set a lifecycle listener for task events.
         */
        public Builder taskLifecycleListener(TaskLifecycleListener taskLifecycleListener) {
            this.taskLifecycleListener = taskLifecycleListener;
            return this;
        }

        public GroupPolicy build() {
            return new GroupPolicy(this);
        }
    }
}
