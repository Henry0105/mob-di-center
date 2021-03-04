package com.youzu.mob.sdkPlus

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object ImprotRegisteredAppkey {
  private val defaultDateFormat = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.CHINA)

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val date: String = args(0)
    val day: String = date.format(defaultDateFormat)
    val url = args(1)
    val table = args(2)

    val spark = SparkSession
      .builder()
      .appName(s"ImprotRegisteredAppkey_$day")
      .config("spark.mongodb.input.uri", url)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    // 从moogo中导入
    val df = MongoSpark.load(spark)
    // 如果mongo中没有任何一条元素,会报字段找不到的错,此处没有做容错。(没有表结构)

    // 只有符合 packageStatus = 1和update_time<计算日期 24:00 的才接受
    val lastTime = LocalDate.parse(date, defaultDateFormat).plusDays(1)
    val accDF = df
      .filter("packageStatus = 1")
      .filter(s"update_time <= '$lastTime'")

    val resDF = accDF.selectExpr("appkey", "apppkg", "create_time", "update_time", s"$day AS day")
    val cnt = resDF.count()

    // 导出到hive
    resDF
      .coalesce((cnt / 1000000).toInt + 1)
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(table)

    logger.info(
      "----------------------------------" +
        s"|  import $cnt lines from mongo.  |" +
        "----------------------------------"
    )

    spark.close()

  }
}
