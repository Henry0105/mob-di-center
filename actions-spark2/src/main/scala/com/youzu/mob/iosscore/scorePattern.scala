package com.youzu.mob.iosscore

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession

class scorePattern(@transient spark: SparkSession) {
  def score(args: Array[String]): Unit = {

  }
}

object scorePattern {
  def main(args: Array[String]): Unit = {
    var day = args(0)
    var seed = args(1)
    var mapping = args(2)
    var output = args(3)
    var model_path = args(4)


    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 1000)

    new scorePattern(spark).score(args)
  }
}
