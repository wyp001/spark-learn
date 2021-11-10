package com.wyp.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {
    // 连接服务器
    val client = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val out: OutputStream = client.getOutputStream
    val objOut = new ObjectOutputStream(out)

    val task = new Task()

    val subTask = new SubTask()
    subTask.logic = task.logic
    subTask.datas = task.datas.take(2)
    objOut.writeObject(subTask)
    objOut.flush()

    // 关闭流和socket连接
    objOut.close()
    client.close()

    val out2: OutputStream = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)

    val subTask2 = new SubTask()
    subTask2.logic = task.logic
    subTask2.datas = task.datas.takeRight(2)
    objOut2.writeObject(subTask2)
    objOut2.flush()

    // 关闭流和socket连接
    objOut2.close()
    objOut2.close()

    println("客户端数据发送完毕")
  }
}
