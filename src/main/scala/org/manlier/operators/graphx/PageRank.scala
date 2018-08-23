package org.manlier.operators.graphx

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.rdd.RDD
import org.manlier.operators.{Op, OpType}
import org.manlier.utils.JsonUtil


/**
  *
  * Create by manlier 2018/8/13 9:17
  */
@Op(`type` = OpType.GRAPHX, name = "pageRank")
class PageRank extends AbsGraphxOperation {

  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("YYYYMMdd")

  override def exec(node: util.Map[String, String], sc: SparkContext, rdd: RDD[_]): RDD[_] = {
    val params: util.Map[String, AnyRef] = JsonUtil.parseToMap(node.get("params").toString)
    val followersPath: String = params.get("edges").toString
    val usersPath: String = params.get("vertices").toString
    val resultPath: Object = params.get("resultPath")

    // Load the edges as a graph
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, followersPath)

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users = sc.textFile(usersPath).map(line => {
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    if (resultPath != null) {
      ranksByUsername.saveAsTextFile(resultPath + File.separator + sc.appName + "-" +
        formatter.format(LocalDateTime.now()))
    }

    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    null
  }


}
