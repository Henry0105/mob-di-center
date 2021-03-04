package com.youzu.mob.score

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object AgeScoring {
  def main(args: Array[String]): Unit = {
    val modelpath = args(0)
    val day = args(2)
    val age_tmp_sql = args(1)
    val URL = args(3)
    val length = args(4).toInt
    val conf = new SparkConf().setAppName(
      this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    System.setProperty("hive.metastore.uris", URL)
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._
    spark.sql(age_tmp_sql).registerTempTable("age_pre")
    val rdd_scoring_age = spark.sql(
      "select device,index,cnt from age_pre where size(index) > 10")
    val rdd_structural_data_scoring_age = rdd_scoring_age.map(r => (
      r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(
        org.apache.spark.mllib.linalg.Vectors.sparse(length,
          r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray)
          .toArray
      ).toSparse
    )
    ).toDF("device", "features")

    val lrModel_age = LogisticRegressionModel.load(modelpath)
    val predictions_age = lrModel_age.transform(rdd_structural_data_scoring_age)

    predictions_age.select("device", "prediction", "probability").map(r => (
      r.getString(0),
      r.getDouble(1),
      r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)
    )
    ).toDF(
      "device", "prediction", "probability").
      registerTempTable("lr_scoring_age")

    spark.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_age_scoring_tmp")
    spark.sql(
      """
        |create table tp_sdk_tmp.result_age_scoring_tmp as
        |select device,(prediction+5) as age,probability from lr_scoring_age
      """.stripMargin)
  }
}