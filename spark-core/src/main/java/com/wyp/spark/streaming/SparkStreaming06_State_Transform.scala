package com.wyp.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)

    /**
     * transform方法可以将底层RDD获取到后进行操作
     * transform 使用场景：
     *    1. DStream功能不完善
     *    2. 需要代码周期性的执行
     */
    /**
     * 此处添加的代码在 Driver端执行
     */
    val newDS: DStream[String] = lines.transform(
      rdd => {
        /**
         * 此处添加的代码在 Driver端执行，（周期性执行）
         */
        rdd.map(
          str => {
            /**
             * 此处添加的代码在 Executor端执行
             */
            str
          }
        )
      }
    )
    /**
     * 此处添加的代码在 Driver端执行
     */
    val newDS1: DStream[String] = lines.map(
      data => {
        /**
         * 此处添加的代码在 Executor端执行
         */
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
