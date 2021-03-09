package com.youzu.mob.score

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable._

object Gender_Scoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val gender_tmp_sql = args(1)
    val day = args(2)
    val conf = new SparkConf().
      setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    sqlContext.sql(gender_tmp_sql).registerTempTable("gender_pre")
    val genderModel = LogisticRegressionModel.load(sc, modelpath + "/gender_model/")

    val temp_rdd_gender = sqlContext.
      sql("SELECT deviceid,id_idx,cnt FROM gender_pre where deviceid is not null and trim(deviceid)<>'' ")
    val test_gender = temp_rdd_gender.map(r => (r.get(0).toString,
        Vectors.sparse(800, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)))
    val predictionAndLabels_gender = test_gender.map {
      r => (r._1, (genderModel.predict(r._2)))
    }
    val results_gender = predictionAndLabels_gender.toDF("device", "gender")
    results_gender.registerTempTable("result_gender_rdd_tmp")
    sqlContext.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_gender_scoring_tmp")
    sqlContext.
      sql("create table tp_sdk_tmp.result_gender_scoring_tmp as select device,gender from result_gender_rdd_tmp")

    sc.stop()
  }
}