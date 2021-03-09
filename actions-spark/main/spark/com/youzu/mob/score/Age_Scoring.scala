package com.youzu.mob.score

import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object Age_Scoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val age_tmp_sql = args(1)
    val day = args(2)
    val length = args(3).toInt
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    sqlContext.sql(age_tmp_sql).registerTempTable("age_pre")
    var age_model = LogisticRegressionModel.load(sc, modelpath + "/age_model/")
    val age_pca = PCAModel.load(modelpath + "/age_pca/")
    val rdd_age_dailysample = sqlContext.sql(
      "select * from age_pre")
      .map(r => (r.getString(0), Vectors.sparse(
        length, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray))
      ).toDF("device", "features")
    val pca_age_dailysample = age_pca.transform(rdd_age_dailysample).select("device", "pcaFeatures").map { r =>
      val prediction = age_model.predict(r.getAs[Vector](1))
      (r.getString(0), prediction)
    }.toDF("device", "age").registerTempTable("result_age_rdd_tmp")

    sqlContext.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_age_scoring_tmp")
    sqlContext.sql("create table tp_sdk_tmp.result_age_scoring_tmp as select device,age from result_age_rdd_tmp")

    sc.stop()
  }
}