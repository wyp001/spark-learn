package com.wyp.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {

  def main(args: Array[String]): Unit = {

    // 启动服务器，接收数据
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")

    // 等待客户端的连接
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    // 转换成接受对象类型数据的流
    val objIn = new ObjectInputStream(in)
    // .asInstanceOf[Task] 将 .readObject()方法返回的对象强转成 Task类型
    val task = objIn.readObject().asInstanceOf[Task]
    val ints = task.compute()
    print("计算节点的计算结果为：" + ints)

    // 关闭流和socket连接
    objIn.close()
    client.close()
    server.close()
  }
}
