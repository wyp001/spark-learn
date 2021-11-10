package com.wyp.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount02 {
  def main(args: Array[String]) = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
    val lines = sc.textFile("datas")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordToSum = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(e=>println(e))
    sc.stop()
  }
}
