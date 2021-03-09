package com.youzu.mob.score

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object Car_Scoring {
  def main(args: Array[String]) {
    val modelpath = args(0)
    val vocation_tmp_sql = args(1)
    val day = args(2)
    val length = args(3).toInt
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    sqlContext.sql(vocation_tmp_sql).registerTempTable("car_pre")

    var car_model = LogisticRegressionModel.load(sc, modelpath + "/car_model/")
    var car_ChiSqSelector = ChiSqSelectorModel.load(sc, modelpath + "/car_ChiSqSelector/")
    var rdd_car_dailysample = sqlContext.sql("select * from car_pre").map { r =>
      var vec = car_ChiSqSelector.
        transform(
          Vectors.sparse(length, r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray).toDense.toSparse)
      (r.getString(0), car_model.predict(vec))
    }.toDF("device", "car").registerTempTable("result_car_rdd_tmp")

    sqlContext.sql("DROP TABLE IF EXISTS tp_sdk_tmp.result_car_scoring_tmp")
    sqlContext.sql("create table tp_sdk_tmp.result_car_scoring_tmp as select device,car from result_car_rdd_tmp")

    sc.stop()
  }
}