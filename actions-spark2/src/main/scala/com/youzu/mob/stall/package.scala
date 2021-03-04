package com.youzu.mob

import com.youzu.mob.stall.LogStallUtil
import org.apache.spark.sql.SparkSession

package object stall {

  // spark参数 初始化
  def initial(name: String, logLevel: String = "ERROR"): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(name)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    // 函数注册
    spark.udf.register("countSum", LogStallUtil.countSum)
    spark
  }
}
