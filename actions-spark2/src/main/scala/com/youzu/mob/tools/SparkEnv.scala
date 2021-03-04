package com.youzu.mob.tools

import org.apache.spark.sql.SparkSession

object SparkEnv {
    def initial(appname: String = "initial_spark_mobdi"): SparkSession = {
      lazy val spark: SparkSession = SparkSession
        .builder()
        .appName(s" ${appname} ")
        .enableHiveSupport()
        .getOrCreate()
      val sc = spark.sparkContext

      sc.setLogLevel("WARN")
      import spark.implicits._
      import spark.sql
      spark
    }
}
