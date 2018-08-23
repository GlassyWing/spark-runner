package org.manlier.task.graphx

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Create by manlier 2018/8/10 11:00
  */
object Aggregate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aggregate").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)

    // Create a graph with "age" as the vertex property. Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100)
        .mapVertices((id, _) => id.toDouble)

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => {
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 +b._1, a._2 + b._2) // Reduce Function
    )

    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) => value match {
        case (count, totalAge) => totalAge / count
      })

    // Display the results
    avgAgeOfOlderFollowers.collect().foreach(println)


  }

}
