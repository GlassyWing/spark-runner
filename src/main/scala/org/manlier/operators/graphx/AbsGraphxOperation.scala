package org.manlier.operators.graphx

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.manlier.operators.Operation

/**
  *
  * Create by manlier 2018/8/14 9:30
  */
abstract class AbsGraphxOperation extends Operation {

  def exec(node: util.Map[String, String], sc: SparkContext, rdd: RDD[_]): RDD[_]

  override def exec(node: util.Map[String, String], ssc: StreamingContext, dstream: DStream[_]): DStream[_] = {
    throw new UnsupportedOperationException()
  }
}
