package com.wyp.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount03 {
  def main(args: Array[String]) = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
    method02(sc)
    sc.stop()
  }


  private def method01(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount = group.mapValues(iter => iter.size)
    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(e => println(e))
  }

  private def method02(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordItem = words.map((_,1))
    val group: RDD[(String, Iterable[Int])] = wordItem.groupByKey()
    val wordCount = group.mapValues(iter => iter.size)

    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(e => println(e))
  }

  private def method03(sc: SparkContext): Unit ={
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordItem: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordItem.aggregateByKey(0)(_ + _, _ + _)

  }



}
