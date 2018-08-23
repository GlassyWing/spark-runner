package org.manlier.task;

import java.util.List;
import java.util.Map;

/**
 * Create by manlier 2018/8/13 11:46
 */
public class Task {

    private String taskName;

    private List<Map<String, String>> taskNodes;

    public Task(String taskName, List<Map<String, String>> taskNodes) {
        this.taskName= taskName;
        this.taskNodes = taskNodes;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public List<Map<String, String>> getTaskNodes() {
        return taskNodes;
    }

    public void setTaskNodes(List<Map<String, String>> taskNodes) {
        this.taskNodes = taskNodes;
    }
}
