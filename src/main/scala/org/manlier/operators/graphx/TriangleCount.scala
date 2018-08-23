package org.manlier.operators.graphx

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.manlier.operators.{Op, OpType}
import org.manlier.utils.JsonUtil

/**
  * 三角计数算法
  * Create by manlier 2018/8/23 9:56
  */
@Op(`type` = OpType.GRAPHX, name = "triangleCount")
class TriangleCount extends AbsGraphxOperation {

  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("YYYYMMdd")

  override def exec(node: util.Map[String, String], sc: SparkContext, rdd: RDD[_]): RDD[_] = {

    val params: util.Map[String, AnyRef] = JsonUtil.parseToMap(node.get("params").toString)
    val followersPath: String = params.get("edges").toString
    val usersPath: String = params.get("vertices").toString
    val resultPath: Object = params.get("resultPath")

    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, followersPath, true).partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = sc.textFile(usersPath).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }

    if (resultPath != null) {
      triCountByUsername.saveAsTextFile(resultPath + File.separator + sc.appName + "-" +
        formatter.format(LocalDateTime.now()))
    }


    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
    null
  }
}
