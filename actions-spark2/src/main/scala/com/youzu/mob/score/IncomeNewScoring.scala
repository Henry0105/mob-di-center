package com.youzu.mob.score

import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.SparkSession

object IncomeNewScoring {

  def main(args: Array[String]): Unit = {

    println(args.mkString(","))
    if (args.length != 5) {
      println(
        s"""
           |ERROR:wrong number of parameters,new model of house need 6 parameters,
           |please check input parameters:
           |<day> <income_pre_sql> <model_path> <length> <mobid_model_par_table>
         """.stripMargin)
    }
    val day = args(0)
    val income_pre_sql = args(1)
    val modelpath = args(2)
    val length = args(3).toInt
    val mobid_model_par_table = args(4)

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import spark.implicits._
    spark.sql(income_pre_sql)
      .registerTempTable("mobdi_income_pre_table_tmp")
    var rdd_scoring = spark.sql("select device,index,cnt from mobdi_income_pre_table_tmp")
    var rdd_structural_data_scoring = rdd_scoring.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            length,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray)
            .toArray).toSparse
      )).toDF("device", "features")


    var rfModel = RandomForestClassificationModel.load(modelpath)
    var predictions_rf = rfModel.setThresholds(Array(1.0, 1.0, 1.1, 0.55, 0.35)).transform(rdd_structural_data_scoring)

    predictions_rf.select("device", "prediction", "probability")
      .map(
        r => (
          r.getString(0), r.getDouble(1),
          r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)
        )
      ).toDF("device", "prediction", "probability")
      .registerTempTable("mobdi_income_rf_scoring")

    val income_out_put =
      s"""
         |INSERT OVERWRITE TABLE $mobid_model_par_table partition(day=$day, kind='income')
         |select device,(prediction+3) as prediction,probability from mobdi_income_rf_scoring
       """.stripMargin
    spark.sql(income_out_put)
  }
}
