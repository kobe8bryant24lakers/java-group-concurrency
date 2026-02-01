package io.github.kobe;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Definition of a task bound to a group.
 */
public record GroupTask<T>(String groupKey, String taskId, Callable<T> task) {

    public GroupTask {
        Objects.requireNonNull(groupKey, "groupKey");
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(task, "task");
    }
}
