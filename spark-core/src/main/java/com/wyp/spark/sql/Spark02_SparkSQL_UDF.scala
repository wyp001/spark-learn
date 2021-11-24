package com.wyp.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * udf 自定义函数
 */
object Spark02_SparkSQL_UDF {

  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // 注册一个udf函数，函数名称为 prefixName， 功能是在 name属性前 加上前缀 Name:
    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })
    // 在spark sql 中使用 自定义的udf函数 prefixName
    spark.sql("select age, prefixName(username) from user").show

    spark.close()
  }
}
