package com.youzu.mob.score

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object MarriedScoring {
  def main(args: Array[String]): Unit = {
    val modelpath = args(0)
    val day = args(2)
    val married_tmp_sql = args(1)
    val URL = args(3)
    val length = args(4).toInt
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    System.setProperty("hive.metastore.uris", URL)
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._

    spark.sql(married_tmp_sql).registerTempTable("married_pre")
    var rdd_scoring_married = spark.sql("select device,index,cnt from married_pre")
    var rdd_structural_data_scoring_married = rdd_scoring_married.map(r => (
      r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(
        org.apache.spark.mllib.linalg.Vectors.sparse(length,
          r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray)
          .toArray).toSparse)
    ).toDF("device", "features")

    var lrModel_married = LogisticRegressionModel.load(modelpath)
    var predictions_married = lrModel_married.setThreshold(0.6).transform(rdd_structural_data_scoring_married)

    predictions_married.
      select("device", "prediction", "probability").map(r => (
      r.getString(0),
      r.getDouble(1),
      r.getAs[org.apache.spark.ml.linalg.Vector](2)
        .apply(r.getDouble(1).toInt)
    )
    ).toDF("device", "prediction", "probability").
      registerTempTable("lr_scoring_married")

    spark.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_married_scoring_tmp")
    spark.sql(
      """
        |create table tp_sdk_tmp.result_married_scoring_tmp as
        |select device,prediction as married,probability from lr_scoring_married
      """.stripMargin)
  }
}