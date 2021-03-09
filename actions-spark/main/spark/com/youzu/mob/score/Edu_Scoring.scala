package com.youzu.mob.score

import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object Edu_Scoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val edu_tmp_sql = args(1)
    val day = args(2)
    val length = args(3).toInt
    val conf = new SparkConf().
      setAppName(
        this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    sqlContext.sql(edu_tmp_sql).registerTempTable("edu_pre")
    var edu_model = LogisticRegressionModel.load(sc, modelpath + "/edu_model/")
    val edu_pca = PCAModel.load(modelpath + "/edu_pca/")
    // edu-model scroing
    val rdd_edu_dailysample = sqlContext.sql("select * from edu_pre").
      map(r => (r.getString(0),
        Vectors.sparse(length, r.getAs[ArrayBuffer[Int]](1).toArray,
        r.getAs[ArrayBuffer[Double]](2).toArray))).toDF("device", "features")
    val pca_edu_dailysample = edu_pca.transform(rdd_edu_dailysample).select("device", "pcaFeatures").map { r =>
      val prediction = edu_model.predict(r.getAs[Vector](1))
      (r.getString(0), prediction)
    }.toDF("device", "edu").registerTempTable("result_edu_rdd_tmp")

    sqlContext.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_edu_scoring_tmp")
    sqlContext.sql("create table tp_sdk_tmp.result_edu_scoring_tmp as select device,edu from result_edu_rdd_tmp")
    sc.stop()
  }
}