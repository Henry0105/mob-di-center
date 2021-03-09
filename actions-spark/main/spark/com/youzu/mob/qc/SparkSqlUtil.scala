package com.youzu.mob.qc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkSqlUtil {
  def main(args: Array[String]) {
    val sql = args(0).trim
    val conf = new SparkConf()
      .set("spark.network.timeout", "600000")
      .set("spark.core.connection.ack.wait.timeout", "600000")
      .set("spark.akka.timeout", "600000")
      .set("spark.storage.blockManagerSlaveTimeoutMs", "600000")
      .set("spark.shuffle.io.connectionTimeout", "600000")
      .set("spark.rpc.askTimeout", "600000")
      .set("spark.rpc.lookupTimeout", "600000")
      .set("spark.sql.broadcastTimeout", "1800")
      .set("spark.executor.heartbeatInterval", "30s")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val sqlArr = sql.split(";")
    for (i <- 0 until sqlArr.length) {
      if (sqlArr(i).trim.length > 3) {
        sqlContext.sql(sqlArr(i))
      }
    }
  }
}