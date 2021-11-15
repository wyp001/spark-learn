package com.wyp.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    // toDebugString 输出 RDD 的血缘关系
    println(lines.toDebugString)
    println("*************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*************************")
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("*************************")
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.toDebugString)
    println("*************************")
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)

    sc.stop()

  }
}
