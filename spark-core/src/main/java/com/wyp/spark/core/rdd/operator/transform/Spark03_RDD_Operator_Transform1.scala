package com.wyp.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 算子 - mapPartitions
 * 输出数据和分区的list 每一个元组里，形如：（分区索引，数据）
 */
object Spark03_RDD_Operator_Transform1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        val rdd = sc.makeRDD(List(1,2,3,4))

        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map(
                    num => {
                        (index, num)
                    }
                )
            }
        )

        mpiRDD.collect().foreach(println)

        sc.stop()

    }
}
