package org.manlier.task.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Create by manlier 2018/8/10 9:03
  */
object Start {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("graphx")
    val sc = SparkContext.getOrCreate(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are postdocs
    graph.vertices.filter({ case (id: VertexId, (name, pos)) => pos == "postdoc" }).count()

    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count()

    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)

    facts.collect.foreach(println)

    val inDegrees: VertexRDD[Int] = graph.inDegrees

    // Given a graph where the vertex property is the out degree
    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))

    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank

    val outputGraph: Graph[Double, Double] =
      inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr)
        .mapVertices((id, _) => 1.0)

  }

}
