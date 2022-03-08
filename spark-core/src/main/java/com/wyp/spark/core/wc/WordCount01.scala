package com.wyp.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount01 {
  def main(args: Array[String]) = {
    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    // 读取文件数据
    val lines = sc.textFile("datas/wc")
    // 将文件中的数据进行分词
    val words = lines.flatMap(_.split(" "))
    // 转换数据结构 word => (word, 1)
    val wordGroup = words.groupBy(word => word)
    val wordCount = wordGroup.map({
      case (word, list) => {
        (word, list.size)
      }
    })
    val array = wordCount.collect()
    array.foreach(println)
    sc.stop()
  }
}
