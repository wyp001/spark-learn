package com.wyp.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform_OuterJoin {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2) , ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5)
    ))

    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    leftJoinRDD.collect().foreach(println)
    println()


    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2)
    ))
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6)
    ))
    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd3.rightOuterJoin(rdd4)
    rightJoinRDD.collect().foreach(println)

    sc.stop()
  }
}
