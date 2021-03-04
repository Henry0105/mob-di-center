package com.youzu.mob.newscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vectors => MlVectors}

import scala.collection.mutable.ArrayBuffer

object AgeScore {

  def main(args: Array[String]): Unit = {
    println(args.mkString(","))

    val modelPath = args(0)
    val modelPath0 = args(1)
    val modelPath1 = args(2)
    val modelPath2 = args(3)
    val pre_sql = args(4)
    val threshold = args(5).split(",").map(_.toDouble)
    val length = args(6).toInt
    val out_put_table = args(7)
    val day = args(8)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val lrModel = LogisticRegressionModel.load(modelPath)

    val pre_data = spark.sql(pre_sql)

    val rdd_structural_data_scoring = pre_data.map(r => {
      val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
      val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
      val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
      (
        r.getAs[String]("device"),
        MlVectors.sparse(length, zipArray._1, zipArray._2)
      )
    }
    ).toDF("device", "features")

    val predictions = lrModel
      .setThresholds(threshold)
      .transform(rdd_structural_data_scoring)

    predictions.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .repartition(10)
      .createOrReplaceTempView("lr_scoring")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = $day, kind = 'agebin')
         |select device,
         |case
         |when prediction = 0 then 5
         |when prediction = 1 then 6
         |when prediction = 2 then 7
         |when prediction = 3 then 8
         |when prediction = 4 then 9
         |else prediction
         |end as prediction, probability
         |from lr_scoring
         |""".stripMargin)

    // 下面是5年段模型
    // 45岁以上模型
    val rdd_structural_data_scoring0_1 = predictions.where("prediction=0")
    val rdd_structural_data_scoring0 = rdd_structural_data_scoring0_1
      .toDF("device", "features", "rawPrediction", "probability", "prediction")
      .select("device", "features")

    val lrModel0 = LogisticRegressionModel.load(modelPath0)
    val predictions0 = lrModel0.transform(rdd_structural_data_scoring0)

    predictions0.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_scoring0")

    // 35-44岁模型
    val rdd_structural_data_scoring1_1 = predictions.where("prediction=1")
    val rdd_structural_data_scoring1 = rdd_structural_data_scoring1_1
      .toDF("device", "features", "rawPrediction", "probability", "prediction")
      .select("device", "features")

    val lrModel1 = LogisticRegressionModel.load(modelPath1)
    val predictions1 = lrModel1.transform(rdd_structural_data_scoring1)

    predictions1.select("device", "prediction", "probability").
      map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_scoring1")

    // 25-34岁模型
    val rdd_structural_data_scoring2_1 = predictions.where("prediction=2")
    val rdd_structural_data_scoring2 = rdd_structural_data_scoring2_1
      .toDF("device", "features", "rawPrediction", "probability", "prediction")
      .select("device", "features")

    val lrModel2 = LogisticRegressionModel.load(modelPath2)
    val predictions2 = lrModel2.transform(rdd_structural_data_scoring2)

    predictions2.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_scoring2")

    spark.sql(
      s"""
         |select device, prediction, probability
         |from $out_put_table
         |where (prediction = 8.0 or prediction = 9.0 or prediction = -1.0) and day = $day and kind = 'agebin'
         |union all
         |select device, prediction, probability
         |from lr_scoring0
         |union all
         |select device, prediction, probability
         |from lr_scoring1
         |union all
         |select device, prediction, probability
         |from lr_scoring2
         |""".stripMargin)
      .repartition(10)
      .createOrReplaceTempView("lr_scoring_1001")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = $day, kind = 'agebin_1001')
         |select device, prediction, probability
         |from lr_scoring_1001
         |""".stripMargin)
  }
}