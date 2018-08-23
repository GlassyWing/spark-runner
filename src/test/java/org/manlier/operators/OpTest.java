package org.manlier.operators;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;
import org.manlier.operators.graphx.PageRank;

import java.util.HashMap;
import java.util.Map;

/**
 * Create by manlier 2018/8/13 9:50
 */
public class OpTest {

    @Test
    public void testPageRank() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("pageRank");
        SparkContext sc = new SparkContext(sparkConf);
        PageRank pageRank = new PageRank();

        Map<String, String> node = new HashMap<>();
        node.put("nodeName", "pageRank");
        node.put("params", "{\"edges\": \"data/graphx/followers.txt\", \"vertices\":\"data/graphx/users.txt\"}");

        pageRank.exec(node, sc, null);
    }
}
