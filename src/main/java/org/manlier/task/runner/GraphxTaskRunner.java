package org.manlier.task.runner;

import org.apache.spark.SparkContext;
import org.manlier.operators.Operation;

import java.util.List;
import java.util.Map;

/**
 * Create by manlier 2018/8/13 10:50
 */
public class GraphxTaskRunner extends AbstractTaskRunner {

    public GraphxTaskRunner(SparkContext sc, Map<String, Class<Operation>> opTable) {
        super(sc, opTable);
    }

    @Override
    public void run(List<Map<String, String>> nodes) {
        for (Map<String, String> node : nodes) {

            Class<Operation> op = opTable.get(node.get("nodeName"));
            try {
                op.newInstance().exec(node, sc, null);
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
