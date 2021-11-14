package com.wyp.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 算子 - map
 */
object Spark01_RDD_Operator_Transform_Part {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 【1，2】，【3，4】
    rdd.saveAsTextFile("output")
    val mapRDD: RDD[Int] = rdd.map(_ * 2)
    // 【2，4】，【6，8】
    mapRDD.saveAsTextFile("output1")

    sc.stop()
  }
}
