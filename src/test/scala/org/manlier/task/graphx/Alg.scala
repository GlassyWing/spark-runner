package org.manlier.task.graphx

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Create by manlier 2018/8/10 11:20
  */
object Alg {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("alg")
    val sc = SparkContext.getOrCreate(conf)

    // Load the edges as a graph
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    graph.triplets.foreach(triplet => println(triplet.srcId + " followed " + triplet.dstId))

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users = sc.textFile("data/graphx/users.txt").map(line => {
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }

}
