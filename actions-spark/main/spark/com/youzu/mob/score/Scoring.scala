package com.youzu.mob.score

import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{LogisticRegressionModel}
import org.apache.spark.ml.feature.{PCAModel}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable._
import org.apache.spark.mllib.feature.{ChiSqSelectorModel}

object Scoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val sql = args(1)
    val conf = new SparkConf().setAppName("Moddel_Scoring")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    var edu_model = LogisticRegressionModel.load(sc, modelpath + "/edu_model/")
    val edu_pca = PCAModel.load(modelpath + "/edu_pca/")
    val rdd_edu_dailysample = sqlContext
      .sql("select * from tp_sdk_tmp.edu_pre")
      .map(r => (r.getString(0), Vectors
        .sparse(1502, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray))).
      toDF("device", "features")
    val pca_edu_dailysample = edu_pca.transform(rdd_edu_dailysample).select("device", "pcaFeatures").map { r =>
      val prediction = edu_model.predict(r.getAs[Vector](1))
      (r.getString(0), prediction)
    }.toDF("device", "score").registerTempTable("result_edu_rdd_tmp")

    var age_model = LogisticRegressionModel.load(sc, modelpath + "/age_model/")
    val age_pca = PCAModel.load(modelpath + "/age_pca/")
    val rdd_age_dailysample = sqlContext.sql(
      "select * from tp_sdk_tmp.age_pre").
      map(r => (r.getString(0), Vectors.
        sparse(1502, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)))
      .toDF("device", "features")
    val pca_age_dailysample = age_pca.transform(rdd_age_dailysample).select("device", "pcaFeatures").map { r =>
      val prediction = age_model.predict(r.getAs[Vector](1))
      (r.getString(0), prediction)
    }.toDF("device", "score").registerTempTable("result_age_rdd_tmp")


    var model_kid = LogisticRegressionModel.load(sc, modelpath + "/kid_model/")
    val rdd_kid_dailysample = sqlContext.
      sql("select * from tp_sdk_tmp.kid_pre").map { r =>
      val vec = Vectors.sparse(
        500, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)
      (r.getString(0), model_kid.predict(vec))
    }.toDF("device", "score").registerTempTable("result_kid_rdd_tmp")

    var model_income = LogisticRegressionModel.load(sc, modelpath + "/income_model/")

    val rdd_income_dailysample = sqlContext.
      sql("select * from tp_sdk_tmp.income_pre").
      map { r =>
      val vec = Vectors.sparse(501, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)
      (r.getString(0), model_income.clearThreshold().predict(vec))
    }.toDF("device", "score")
      .registerTempTable("result_income_rdd_tmp")

    val genderModel = LogisticRegressionModel.load(sc, modelpath + "/gender_model/")

    val temp_rdd_gender = sqlContext.
      sql("SELECT deviceid,id_idx,cnt FROM tp_sdk_tmp.gender_pre where deviceid is not null and trim(deviceid)<>'' ")
    val test_gender = temp_rdd_gender.map(r => (r.get(0).toString,
        Vectors.sparse(800, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray)))
    val predictionAndLabels_gender = test_gender.map {
      r => (r._1, (genderModel.predict(r._2)))
    }
    val results_gender = predictionAndLabels_gender.toDF("deviceid", "gender")
    results_gender.registerTempTable("result_gender_rdd_tmp")
    var vocation_model = LogisticRegressionModel.load(sc, modelpath + "/vocation_model/")
    var vocation_ChiSqSelector = ChiSqSelectorModel.load(sc, modelpath + "/vocation_ChiSqSelector/")

    var rdd_vocation_dailysample = sqlContext.sql("select * from tp_sdk_tmp.vocation_pre").map { r =>
      var vec = vocation_ChiSqSelector.
        transform(
          Vectors.sparse(4443, r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray).toDense.toSparse)
      (r.getString(0), vocation_model.predict(vec))
    }.toDF("device", "score").registerTempTable("result_vocation_rdd_tmp")
    var car_model = LogisticRegressionModel.load(sc, modelpath + "/car_model/")
    var car_ChiSqSelector = ChiSqSelectorModel.load(sc, modelpath + "/car_ChiSqSelector/")
    var rdd_car_dailysample = sqlContext.sql("select * from tp_sdk_tmp.car_pre").map { r =>
      var vec = car_ChiSqSelector.transform(
        Vectors.sparse(1503, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toDense.toSparse)
      (r.getString(0), car_model.predict(vec))
    }.toDF("device", "score").registerTempTable("result_car_rdd_tmp")
    println("sql----->" + sql)
    sqlContext.sql(sql);
    sc.stop()
  }
}