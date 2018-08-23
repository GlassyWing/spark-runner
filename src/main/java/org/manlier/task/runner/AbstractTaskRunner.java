package org.manlier.task.runner;

import org.apache.spark.SparkContext;
import org.manlier.operators.Operation;

import java.util.List;
import java.util.Map;

/**
 * Create by manlier 2018/8/13 11:06
 */
public abstract class AbstractTaskRunner {

    protected Map<String, Class<Operation>> opTable;

    protected SparkContext sc;

    public AbstractTaskRunner(SparkContext sc, Map<String, Class<Operation>> opTable) {
        this.sc = sc;
        this.opTable = opTable;
    }

    public abstract void run(List<Map<String, String>> nodes);

}
