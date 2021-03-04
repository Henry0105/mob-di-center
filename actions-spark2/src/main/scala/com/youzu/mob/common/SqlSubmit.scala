package com.youzu.mob.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SqlSubmit {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sqlStr = args(0).trim
    val subSqlStr = if (sqlStr.length > 50) sqlStr.substring(0, 50) + "..." else sqlStr

    val conf = new SparkConf().setAppName("SparkSqlSubmit - " + subSqlStr)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    for (exeSql <- sqlStr.split(";")) {
      println(exeSql)
      spark.sql(exeSql)
    }

    spark.stop()
  }
}
