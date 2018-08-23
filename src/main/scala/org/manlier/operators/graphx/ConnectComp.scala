package org.manlier.operators.graphx

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.manlier.operators.{Op, OpType}
import org.manlier.utils.JsonUtil

/**
  * 连通体算法
  *
  * Create by manlier 2018/8/23 9:44
  */
@Op(`type` = OpType.GRAPHX, name = "connectedComponents")
class ConnectComp extends AbsGraphxOperation {

  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("YYYYMMdd")

  override def exec(node: util.Map[String, String], sc: SparkContext, rdd: RDD[_]): RDD[_] = {
    val params: util.Map[String, AnyRef] = JsonUtil.parseToMap(node.get("params").toString)
    val followersPath: String = params.get("edges").toString
    val usersPath: String = params.get("vertices").toString
    val resultPath: Object = params.get("resultPath")

    val graph = GraphLoader.edgeListFile(sc, followersPath)
    // Find the connected components
    val cc = graph.connectedComponents().vertices

    // Join the connected components with the username

    // Join the ranks with the usernames
    val users = sc.textFile(usersPath).map(line => {
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    val ccByUsername = users.join(cc).map {
      case (id, (username, c)) => (username, c)
    }

    if (resultPath != null) {
      ccByUsername.saveAsTextFile(resultPath + File.separator + sc.appName + "-" +
        formatter.format(LocalDateTime.now()))
    }

    // Print the result
    println(ccByUsername.collect().mkString("\n"))

    null
  }
}
