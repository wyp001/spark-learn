package com.wyp.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 算子 - mapPartitions
 * 获取第二个分区的数据
 */
object Spark03_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 【1，2】，【3，4】
    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          // 返回一个空的迭代器
          Nil.iterator
        }
      }
    )
    mpiRDD.collect().foreach(println)


    sc.stop()

  }
}
