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
    val i = in.read()
    print("接收到客户端发送的数据: " + i)

    // 关闭流和socket连接
    in.close()
    client.close()
    server.close()
  }
}
