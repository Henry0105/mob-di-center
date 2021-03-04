package com.youzu.mob.score

import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.linalg.{Vector => mlVector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vector => mllibVector, Vectors => mllibVectors}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object EduScoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val day = args(2)
    val edu_tmp_sql = args(1)
    val URL = args(3)
    val length = args(4).toInt
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    System.setProperty("hive.metastore.uris", URL)
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._
    spark.sql(edu_tmp_sql).registerTempTable("edu_pre")

    val edu_model = LogisticRegressionModel.load(modelpath)
    edu_model.setThresholds(Array(0.75, 0.55, 0.45, 0.90))
    val rdd_edu = spark.sql("select device,index,cnt from edu_pre ")
      .map(r =>
      (r.getString(0),
        mlVectors.dense(
          mllibVectors.sparse(
            length, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray
        ).toSparse
      )
    ).toDF("device", "features")

    val edu_full = edu_model.transform(rdd_edu.repartition(1000)).select("device", "prediction")

    edu_full.registerTempTable("result_edu_rdd_tmp")

    spark.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_edu_scoring_tmp")
    spark.sql(
      """
        |create table tp_sdk_tmp.result_edu_scoring_tmp as
        |select device,(prediction+6) as edu from result_edu_rdd_tmp
      """.stripMargin)
  }
}