package com.youzu.mob.score

import com.youzu.mob.utils.Constants.TP_SDK_TMP
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Vector => mlVector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vector => mllibVector, Vectors => mllibVectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object GenderScoring {
  def main(args: Array[String]): Unit = {
    val modelpath = args(0)
    val day = args(2)
    val gender_tmp_sql = args(1)
    val URL = args(3)
    val length = args(4).toInt
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    System.setProperty("hive.metastore.uris", URL)
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._
    spark.sql(gender_tmp_sql).registerTempTable("gender_pre")

    val gender_model = LogisticRegressionModel.load(modelpath)
    gender_model.setThreshold(0.43)

    val rdd_gender_full = spark.sql("select device,index,cnt from gender_pre")
      .map(r =>
        (r.getString(0),
          mlVectors.dense(
            mllibVectors.sparse(
              length, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray
          ).toSparse
        )
      ).toDF("device", "features")
    gender_model.transform(rdd_gender_full).
      select("device", "prediction", "probability").
      registerTempTable("result_gender_rdd_tmp")

    spark.sql(s"DROP TABLE IF EXISTS $TP_SDK_TMP.result_gender_scoring_tmp")
    spark.sql(
      s"""
        |create table $TP_SDK_TMP.result_gender_scoring_tmp as
        |select device,prediction as gender from result_gender_rdd_tmp
      """.stripMargin)
  }
}