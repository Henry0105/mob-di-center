package com.youzu.mob.score

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object Vocation_Scoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val vocation_tmp_sql = args(1)
    val day = args(2)
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    sqlContext.sql(vocation_tmp_sql).registerTempTable("vocation_pre")
    var vocation_model = LogisticRegressionModel.load(sc, modelpath + "/vocation_model/")
    var vocation_ChiSqSelector = ChiSqSelectorModel.load(sc, modelpath + "/vocation_ChiSqSelector/")

    var rdd_vocation_dailysample = sqlContext.sql("select * from vocation_pre").map { r =>
      var vec = vocation_ChiSqSelector.
        transform(
          Vectors.sparse(
            4443, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toDense.toSparse)
      (r.getString(0), vocation_model.predict(vec))
    }.toDF("device", "vocation").registerTempTable("result_vocation_rdd_tmp")

    sqlContext.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_vocation_scoring_tmp")
    sqlContext.
      sql("create table tp_sdk_tmp.result_vocation_scoring_tmp as select device,vocation from result_vocation_rdd_tmp")
    sc.stop()
  }
}