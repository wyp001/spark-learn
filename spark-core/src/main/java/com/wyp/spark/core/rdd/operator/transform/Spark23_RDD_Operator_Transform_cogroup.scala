package com.wyp.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform_cogroup {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2) //, ("c", 3)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6), ("c", 7)
    ))

    // cogroup : connect + group (分组，连接)
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    cgRDD.collect().foreach(println)

    sc.stop()
  }
}
