package org.manlier.operators

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  *
  * Create by manlier 2018/8/13 9:18
  */
trait Operation {

  def exec(node: util.Map[String, String], sc: SparkContext, rdd: RDD[_]): RDD[_]

  def exec(node: util.Map[String, String], ssc: StreamingContext, dstream: DStream[_]): DStream[_]

}
