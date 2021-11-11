package com.wyp.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {

  def main(args: Array[String]): Unit = {

    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    // 指定并行分区数, 程序执行后会在 rdd.saveAsTextFile() 指定的文件加下生成多个分区文件
    //    sparkConf.set("spark.default.parallelism", "5");
    val sc = new SparkContext(sparkConf)

    // 创建RDD
    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    //val rdd = sc.textFile("datas/1.txt")
    // 如果不想使用默认的分区数量，可以通过参数 minPartitions 指定分区数
    // minPartitions不指定，那么makeRDD方法会使用默认值：默认取值为当前运行环境的最大可用核数

    // Spark读取文件，底层其实使用的就是Hadoop的读取方式
    // 分区数量的计算方式：
    //    totalSize = 7
    //    goalSize =  7 / 2 = 3（byte）

    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区)

    //
    //    val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    rdd.saveAsTextFile("output")


    // TODO 关闭环境
    sc.stop()
  }
}
