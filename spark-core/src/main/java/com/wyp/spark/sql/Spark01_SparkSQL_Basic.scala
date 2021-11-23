package com.wyp.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {

    // 1、创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 2、执行逻辑操作
    // 构建 DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    //df.show()

    // DataFrame => SQL
    //df.createOrReplaceTempView("user")
    //spark.sql("select * from user").show()
    //spark.sql("select age, username from user").show()
    //spark.sql("select avg(age) from user").show()

    // DataFrame => DSL
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._    // 引入隐式转换规则，其中的spark 为 SparkSession 的实例名称
    df.select("age", "username").show
    df.select($"age" + 1).show
    df.select('age + 1).show

    // /3、关闭环境
    spark.close()
  }
}
