package com.wyp.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {

    // 1、创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._    // 引入隐式转换规则，其中的spark 为 SparkSession 的实例名称

    // 2、执行逻辑操作
    // 构建 DataFrame
    //val df: DataFrame = spark.read.json("datas/user.json")
    //df.show()

    // DataFrame => SQL
    //df.createOrReplaceTempView("user")
    //spark.sql("select * from user").show()
    //spark.sql("select age, username from user").show()
    //spark.sql("select avg(age) from user").show()

    // DataFrame => DSL
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    //df.select("age", "username").show
    //df.select($"age" + 1).show
    //df.select('age + 1).show

    // DataSet
    // 构建DataSet
    // DataFrame其实是特定泛型的DataSet
    //val seq = Seq(1, 2, 3, 4)
    //val ds: Dataset[Int] = seq.toDS()
    //ds.show()

    // RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    // RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD: RDD[User] = ds1.rdd

    // /3、关闭环境
    spark.close()
  }

  case class User( id:Int, name:String, age:Int )


}
