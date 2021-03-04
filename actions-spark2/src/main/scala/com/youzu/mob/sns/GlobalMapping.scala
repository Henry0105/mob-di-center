package com.youzu.mob.sns

import org.apache.spark.sql.SparkSession

object GlobalMapping {
  def main(args: Array[String]): Unit = {
    require(args.length == 1)

    val sql = args(0).trim

    val spark = SparkSession
      .builder()
      .config("spark.sql.orc.filterPushdown", true)
      .appName("GlobalMapping")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("merge", new MergeSNSUDAF("snsplat", "snsuid", "maxday"))

    spark.sql(sql)

    spark.stop()
  }
}