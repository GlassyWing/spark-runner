package org.manlier.task.runner;

import java.util.List;
import java.util.Map;

/**
 * 任务执行器
 * Create by manlier 2018/8/13 11:05
 */
public interface TaskRunner {

    void run(List<Map<String, String>> nodes);
}
