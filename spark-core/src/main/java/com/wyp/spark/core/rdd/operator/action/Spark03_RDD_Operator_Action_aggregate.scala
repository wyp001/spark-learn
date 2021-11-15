package com.wyp.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action_aggregate {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    //10 + 13 + 17 = 40
    // aggregateByKey : 初始值只会参与分区内计算
    // aggregate : 初始值会参与分区内计算,并且和参与分区间计算
//    val result = rdd.aggregate(10)(_ + _, _ + _)

    // 当aggregate 指定的分区内和分区间的计算方式一样时，可以使用 fold 算子进行简化
    val result = rdd.fold(10)(_ + _)

    println(result)

    sc.stop()
  }
}
