package com.wyp.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * 176.SparkSQL - 核心编程 - 数据读取和保存 - 操作MySQL
 */
object Spark04_SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取MySQL数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/db_spark_learn")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "t_user")
      .load()
    df.show

    /**
     * 保存数据
     * 将上面读取到的数据 保存到 db_spark_learn 数据库中的 t_user_copy 表中，t_user_copy表没有则创建
     */
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/db_spark_learn")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "t_user_copy")
      .mode(SaveMode.Append)
      .save()

    spark.close()
  }
}
