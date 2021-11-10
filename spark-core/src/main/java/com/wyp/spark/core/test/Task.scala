package com.wyp.spark.core.test

class Task extends Serializable {

  val datas = List(1, 2, 3, 4)

// 等价于  val logic = ( num:Int )=>{ num * 2 }
//  也等价于
//  val logic: (Int) => Int = {
//    i => i * 2
//  }
  val logic: (Int) => Int = _ * 2

  // 计算
    def compute() = {
      datas.map(logic)
    }
  //等价于：
//    def compute(): List[Int] = {
//    datas.map(logic)
//  }
  //等价于：
//  def compute() {
//    datas.map(logic)
//  }


}
