package org.manlier.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create by manlier 2018/8/10 17:37
 */
public class TaskBuilder {

    private String hadoopHost;
    private String xmlPath;

    public TaskBuilder(String hadoopHost, String xmlPath) {
        this.hadoopHost = hadoopHost;
        this.xmlPath = xmlPath;
    }

    private List<Map<String, String>> getAllNodes(Elements nodes) {
        return nodes.parallelStream().map(e -> {
            Map<String, String> kv = new HashMap<>();
            Elements children = e.children();
            for (int i = 0; i < children.size(); i += 2) {
                kv.put(children.get(i).text(), children.get(i + 1).text());
            }
            return kv;
        }).collect(Collectors.toList());
    }

    public Task buildTask() {
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            FileSystem fs = FileSystem.get(URI.create(hadoopHost), conf);
            try (InputStream in = fs.open(new Path(xmlPath))) {
                Document document = Jsoup.parse(in, null, "", Parser.xmlParser());
                String taskName = document.select("properties").attr("appName");
                List<Map<String, String>> taskNodes = getAllNodes(document.select("node"));
                return new Task(taskName, taskNodes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
