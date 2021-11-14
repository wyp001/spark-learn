package com.wyp.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 算子 - groupBy
 * List("Hello", "Spark", "Scala", "Hadoop") 根据每个单词的首字母作为key 进行分组
 */
object Spark06_RDD_Operator_Transform1_GroupBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"), 2)

    // 分组和分区没有必然的关系
    val groupRDD = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
