package org.manlier.task;

import org.junit.Before;
import org.junit.Test;
import org.manlier.operators.OperationScanner;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Create by manlier 2018/8/13 11:33
 */
public class TaskDriverTest {


    public static void main(String[] args) {
        TaskBuilder builder = new TaskBuilder("file://C:", "C:/Users/14902/Desktop/pageRank_1.xml");
        Task task = builder.buildTask();
        System.out.println(task.getTaskNodes());
        TaskDriver taskDriver = new TaskDriver(builder.buildTask());
        taskDriver.start();
        System.out.println(new OperationScanner("org.manlier.operators").getAllOperations());

    }
}