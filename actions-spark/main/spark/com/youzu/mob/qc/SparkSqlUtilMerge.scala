package com.youzu.mob.qc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkSqlUtilMerge {
  def main(args: Array[String]) {
    val sql1 = args(0).trim
    val sql2 = args(1).trim
    val repartition = args(2).toInt
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
    if (sql1.length > 3 && sql2.length > 3) {
      sqlContext.sql(sql1).registerTempTable("tmp")
      sqlContext.sql("cache table cahceTmp as select * from tmp")
      sqlContext.sql("select * from cahceTmp")
        .coalesce(repartition).registerTempTable("cahceTmp_repartition")
      sqlContext.sql(
        s"""
           |${sql2}
           |select * from cahceTmp_repartition
          """.stripMargin)
    }
  }
}