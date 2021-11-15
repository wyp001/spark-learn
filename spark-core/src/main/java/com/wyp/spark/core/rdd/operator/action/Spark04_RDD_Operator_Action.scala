package com.wyp.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,1,1,4),2)
    val intToLong: collection.Map[Int, Long] = rdd1.countByValue()
    println(intToLong) // 输出 Map(4 -> 1, 1 -> 3)  表示 元素 4 出现 1次； 元素 1 出现 3次
    println()

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))
    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
    println(stringToLong) // Map(a -> 3) 表示元组中 key 为 a 的出现 3次

    sc.stop()
  }
}
