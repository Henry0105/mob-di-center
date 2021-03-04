package com.youzu.mob.score

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object KidsScoring {
  def main(args: Array[String]): Unit = {
    val modelpath = args(0)
    val day = args(2)
    val kids_tmp_sql = args(1)
    val length = args(3).toInt
    val URL = args(4)
    val conf = new SparkConf().setAppName(
      this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")

    System.setProperty("hive.metastore.uris", URL)

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sql(kids_tmp_sql).registerTempTable("kids_pre")
    val rdd_scoring_kids = spark.sql("select device,index,cnt from kids_pre")
    val rdd_structural_data_scoring_kids = rdd_scoring_kids.map(r => (
      r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(length,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)
        .toArray).toSparse)).toDF("device", "features")
    val lrModel_kids = LogisticRegressionModel.load(modelpath)
    val predictions_kids = lrModel_kids.transform(rdd_structural_data_scoring_kids)

    predictions_kids.select("device", "prediction", "probability").map(r => (
      r.getString(0),
      r.getDouble(1),
      r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt))
    ).toDF("device", "prediction", "probability").registerTempTable("lr_scoring_kids")

    spark.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_kids_scoring_tmp")
    spark.sql(
      """
        |create table tp_sdk_tmp.result_kids_scoring_tmp as
        |select device,case when prediction = 0 then 2
        |       when prediction = 1 then 3
        |       when prediction = 2 then 4
        |       when prediction = 3 then 5
        |       when prediction = 4 then 3
        |       when prediction = 5 then 4
        |       when prediction = 6 then 5
        |       else 2 end as kids,probability
        |       from lr_scoring_kids
      """.stripMargin)
  }

}