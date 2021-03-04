package com.youzu.mob.tools

import org.apache.spark.sql.SparkSession

object SparkSqlUtil {
  def main(args: Array[String]): Unit = {
    val sql = args(0).trim
    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.network.timeout", "600000")
      .config("spark.core.connection.ack.wait.timeout", "600000")
      .config("spark.akka.timeout", "600000")
      .config("spark.storage.blockManagerSlaveTimeoutMs", "600000")
      .config("spark.shuffle.io.connectionTimeout", "600000")
      .config("spark.rpc.askTimeout", "600000")
      .config("spark.rpc.lookupTimeout", "600000")
      .config("spark.sql.broadcastTimeout", "1800")
      .config("spark.executor.heartbeatInterval", "30s").getOrCreate()

    val sqlArr = sql.split(";")
    for (i <- 0 until sqlArr.length) {
      if (sqlArr(i).trim.length > 3) {
        spark.sql(sqlArr(i))
      }
    }
  }
}
