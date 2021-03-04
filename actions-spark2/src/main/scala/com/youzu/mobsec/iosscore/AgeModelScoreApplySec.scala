package com.youzu.mobsec.iosscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object AgeModelScoreApplySec {

  // 模型计算得分
  def main(args: Array[String]): Unit = {

    // 参数导入
    val iosage_ifid_scoredata_1 = args(0)
    val day = args(1)
    val iosage_score1 = args(2)
    val path = args(3)

    val spark = SparkSession
      .builder()
      .appName(s"age_score_apply")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val rdd_score = spark.sql(
      s"""
         |   select ifid, tag from ${iosage_ifid_scoredata_1} where day = '${day}'
         """.stripMargin)
    val value = rdd_score.map(
      r => {
        (r.getString(0),
          r.getString(1).split("=")(0).split(",").map(_.toInt),
          r.getString(1).split("=")(1).split(",").map(_.toDouble)
        )
      }
    ).toDF("ifid", "index", "cnt1")

    val lrModel = LogisticRegressionModel.load(s"${path}")

    var scoreData = value.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(
        org.apache.spark.mllib.linalg.Vectors.sparse(900,
          r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray)
          .toArray
      ).toSparse)
    ).toDF("ifid", "features")

    lrModel.setThresholds(Array(0.9, 1, 1.6, 1.2, 1))
    var predictions_score = lrModel.transform(scoreData)


    predictions_score.select("ifid", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("ifid", "prediction", "probability")
      .createOrReplaceTempView("tmpTable2")
    spark.sql(
      s"""
         |drop table if exists ${iosage_score1}
        """.stripMargin)
    spark.sql(
      s"""
         |create table ${iosage_score1} stored as orc as
         |select * from tmpTable2
  """.stripMargin)

  }
}
