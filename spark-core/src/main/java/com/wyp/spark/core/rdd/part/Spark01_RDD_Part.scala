package com.wyp.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxx111xxxxx"),
      ("cba", "xxxx222xxxxx"),
      ("wnba", "xxx333xxxxxx"),
      ("nba", "xxxx444xxxxx"),
    ), 3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /**
   * 自定义分区器
   * 1. 继承Partitioner
   * 2. 重写方法
   */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }

      //等价于：
      //if (key == "nba") {
      //  0
      //} else if (key == "wnba") {
      //  1
      //} else {
      //  2
      //}

    }
  }

}
