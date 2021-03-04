package com.youzu.mob.tools

import org.apache.spark.sql.SparkSession

object SparkSqlUtilMerge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.network.timeout", "600000")
      .config("spark.core.connection.ack.wait.timeout", "600000")
      .config("spark.akka.timeout", "600000")
      .config("spark.storage.blockManagerSlaveTimeoutMs", "600000")
      .config("spark.shuffle.io.connectionTimeout", "600000")
      .config("spark.rpc.askTimeout", "600000")
      .config("spark.rpc.lookupTimeout", "600000")
      .config("spark.sql.broadcastTimeout", "1800")
      .config("spark.executor.heartbeatInterval", "30s")
      .appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    val sql1 = args(0).trim
    val sql2 = args(1).trim
    val repartition = args(2).toInt
    if (sql1.length > 3 && sql2.length > 3) {
      spark.sql(sql1).registerTempTable("tmp")
      spark.sql("cache table cahceTmp as select * from tmp")
      spark.sql("select * from cahceTmp").coalesce(repartition).registerTempTable("cahceTmp_repartition")
      spark.sql(
        s"""
           |${sql2}
           |select * from cahceTmp_repartition
          """.stripMargin)
    }
  }
}
