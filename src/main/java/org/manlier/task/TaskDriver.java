package org.manlier.task;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.manlier.operators.OpType;
import org.manlier.operators.Operation;
import org.manlier.operators.OperationScanner;
import org.manlier.task.runner.GraphxTaskRunner;

import java.util.List;
import java.util.Map;

/**
 * 任务驱动器，驱动任务执行
 * <p>
 * Create by manlier 2018/8/13 10:02
 */
public class TaskDriver {

    private List<Map<String, String>> nodes;
    private Map<OpType, Map<String, Class<Operation>>> allOperations;
    private String appName;

    public TaskDriver(List<Map<String, String>> taskNodes, String appName) {
        this.nodes = taskNodes;
        this.appName = appName;
        this.allOperations = new OperationScanner("org.manlier.operators").getAllOperations();
    }

    public TaskDriver(Task task) {
        this(task.getTaskNodes(), task.getTaskName());
    }

    public void start() {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("yarn");
        SparkContext sc = new SparkContext(conf);
        switch (getType(nodes.get(0))) {
            case GRAPHX:
                new GraphxTaskRunner(sc, allOperations.get(OpType.GRAPHX)).run(nodes);
                break;
        }
        sc.stop();
    }

    private OpType getType(Map<String, String> node) {
        return OpType.valueOf(node.get("type").toUpperCase());
    }

    public static void main(String[] args) {

        String host = args[0];
        String xmlPath = args[1];

        TaskBuilder builder = new TaskBuilder(host, xmlPath);
        TaskDriver taskDriver = new TaskDriver(builder.buildTask());
        taskDriver.start();
    }
}
