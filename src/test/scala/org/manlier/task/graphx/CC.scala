package org.manlier.task.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

/**
  *
  * Create by manlier 2018/8/23 9:49
  */
object CC {

  def main(args: Array[String]): Unit = {
    val sConf = new SparkConf().setAppName("cc").setMaster("local[1]")
    val sc = new SparkContext(sConf)
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices

    // Join the connected components with the username

    // Join the ranks with the usernames
    val users = sc.textFile("data/graphx/users.txt").map(line => {
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    val ccByUsername = users.join(cc).map {
      case (id, (username, c)) => (username, c)
    }

    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }

}
