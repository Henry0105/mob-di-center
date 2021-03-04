package com.youzu.mob.newscore

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vectors => MlVectors}

object IndustryScore {

  def main(args: Array[String]): Unit = {

    val model_path_arr = args(0).split(";")
    val thresholds_arr = args(1).split(";")
    val pre_sql_arr = args(2).split(";")
    val length = args(3).toInt
    val out_put_table = args(4)
    val day = args(5)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val lrModel_whitecollar = LogisticRegressionModel.load(model_path_arr(0))
    val lrModel_service = LogisticRegressionModel.load(model_path_arr(1))
    val lrModel_bluecollar = LogisticRegressionModel.load(model_path_arr(2))
    val lrModel_individual = LogisticRegressionModel.load(model_path_arr(3))

    val df_scoring_whitecollar = spark.sql(pre_sql_arr(0))
    val df_structural_data_scoring_whitecollar = df_scoring_whitecollar
      .map(r => {
        val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
        val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
        val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
        (
          r.getAs[String]("device"),
          MlVectors.sparse(length, zipArray._1, zipArray._2)
        )
      })
      .toDF("device", "features")

    val df_scoring_service = spark.sql(pre_sql_arr(1))
    val df_structural_data_scoring_service = df_scoring_service
      .map(r => {
        val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
        val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
        val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
        (
          r.getAs[String]("device"),
          MlVectors.sparse(length, zipArray._1, zipArray._2)
        )
      })
      .toDF("device", "features")

    val df_scoring_bluecollar = spark.sql(pre_sql_arr(2))
    val df_structural_data_scoring_bluecollar = df_scoring_bluecollar
      .map(r => {
        val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
        val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
        val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
        (
          r.getAs[String]("device"),
          MlVectors.sparse(length, zipArray._1, zipArray._2)
        )
      })
      .toDF("device", "features")

    val df_scoring_individual = spark.sql(pre_sql_arr(3))
    val df_structural_data_scoring_individual = df_scoring_individual
      .map(r => {
        val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
        val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
        val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
        (
          r.getAs[String]("device"),
          MlVectors.sparse(length, zipArray._1, zipArray._2)
        )
      })
      .toDF("device", "features")

    val predictions_whitecollar = lrModel_whitecollar.setThresholds(thresholds_arr(0).split(",").map(_.toDouble))
      .transform(df_structural_data_scoring_whitecollar)

    val predictions_service = lrModel_service.setThresholds(thresholds_arr(1).split(",").map(_.toDouble)).
      transform(df_structural_data_scoring_service)

    val predictions_bluecollar = lrModel_bluecollar.setThresholds(thresholds_arr(2).split(",").map(_.toDouble))
      .transform(df_structural_data_scoring_bluecollar)

    val predictions_individual = lrModel_individual.setThresholds(thresholds_arr(3).split(",").map(_.toDouble))
      .transform(df_structural_data_scoring_individual)


    val predictions = predictions_whitecollar
      .union(predictions_service).union(predictions_bluecollar).union(predictions_individual)


    predictions.select("device", "prediction", "probability")
      .map(r =>
        (r.getString(0), r.getDouble(1),
          r.getAs[org.apache.spark.ml.linalg.Vector](2).
            apply(r.getDouble(1).toInt))
      ).toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_industry_scoring")

    spark.sql(
      s"""
         |select device, prediction, probability
         |from
         |(
         |select a.device,
         |case
         |when b.prediction = 19 and a.probability <= 0.7 then 12
         |when b.prediction = 18 and a.probability <= 0.7 then 2
         |else a.prediction
         |end as prediction, a.probability
         |from lr_industry_scoring as a
         |left join $out_put_table as b
         |on a.device = b.device and b.day = $day and b.kind = 'occupation'
         |
         |union all
         |
         |select device,
         |case
         |when prediction = 14 then 10
         |when prediction = 15 then 3
         |when prediction = 16 then 6
         |else 11
         |end as prediction, probability
         |from $out_put_table
         |where day = $day and kind = 'occupation'
         |and prediction in (14, 15, 16, 20)
         |) as b
         |""".stripMargin)
      .repartition(10)
      .createOrReplaceTempView("lr_industry_scoring_final")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = $day, kind = 'industry')
         |select device, prediction, probability
         |from lr_industry_scoring_final
         |""".stripMargin)
  }
}
