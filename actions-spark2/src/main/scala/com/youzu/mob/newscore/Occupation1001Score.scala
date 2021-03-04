package com.youzu.mob.newscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Vectors => MlVectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Occupation1001Score {

  def main(args: Array[String]): Unit = {

    println(args.mkString(","))
    val modelPath = args(0)
    val pre_sql = args(1)
    val thresholds = args(2).split(",").map(_.toDouble)
    val length = args(3).toInt
    val out_put_table = args(4)
    val day = args(5)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    val model = LogisticRegressionModel.load(modelPath)

    val pre_data = spark.sql(pre_sql)
      .map(r => {
        val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
        val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
        val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
        (
          r.getAs[String]("device"),
          MlVectors.sparse(length, zipArray._1, zipArray._2)
        )
      }
      ).toDF("device", "features")

    val data_training = model.setThresholds(thresholds).transform(pre_data)

    data_training.select("device", "prediction", "probability")
      .map(r =>
        (r.getString(0), r.getDouble(1),
          r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt))
      ).toDF("device", "prediction", "probability")
      .repartition(10)
      .createOrReplaceTempView("lr_occupation_1001_scoring")

    spark.sql(
      s"""
         | insert overwrite table $out_put_table partition(day = $day, kind='occupation_1001')
         | select device, (prediction + 13) as prediction, probability
         | from lr_occupation_1001_scoring
      """.stripMargin)
  }
}
