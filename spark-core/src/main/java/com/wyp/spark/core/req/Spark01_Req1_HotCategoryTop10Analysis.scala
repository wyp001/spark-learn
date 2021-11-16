package com.wyp.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Top 10 热门品类
 */
object Spark01_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类ID，点击数量）
    // 过滤掉不是点击操作的数据（行）
    val clickActionRDD: RDD[String] = actionRDD.filter(line => {
      val items: Array[String] = line.split("_")
      val actionType = items(6)
      actionType != "-1"
    })
    // 将数据转换成 （品类ID，点击数量） 元组
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(line => {
      val items: Array[String] = line.split("_")
      (items(6), 1)
    })

    // 3. 统计品类的下单数量：（品类ID，下单数量）
    // 过滤掉下单量为 null 的 数据
    val orderActionRDD = actionRDD.filter(line => {
      val items = line.split("_")
      val orderNum = items(8)
      val flag = orderNum != "null"
      flag
    })
    // 品类id,下单量
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(line => {
      val items = line.split("_")
      val cids: String = items(8) // 品类id
      val cidList: Array[String] = cids.split(",")
      //val tuples = cidList.map(cid => (cid, 1))
      //tuples
      cidList.map(cid => (cid, 1))
    }).reduceByKey(_+_)

    // 4. 统计品类的支付数量：（品类ID，支付数量）
    val payActionRDD: RDD[String] = actionRDD.filter(line => {
      val items = line.split("_")
      val payCount = items(10)
      payCount != "null"
    })
    // （品类ID，支付数量）
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(line => {
      val items = line.split("_")
      val cids: String = items(10)
      val cidList: Array[String] = cids.split(",")
      cidList.map(id => (id, 1))
    })

    // 5. 将品类进行排序，并且取前10名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )




    // 6. 将结果采集到控制台打印出来




    sc.stop()
  }
}
