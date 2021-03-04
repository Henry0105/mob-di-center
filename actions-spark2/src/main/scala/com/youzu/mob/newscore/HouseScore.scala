package com.youzu.mob.newscore

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vectors => MlVectors}

object HouseScore {

  def main(args: Array[String]): Unit = {
    println(args.mkString(","))

    val modelPath = args(0)
    val pre_sql = args(1)
    val threshold = args(2).toDouble
    val length = args(3).toInt
    val out_put_table = args(4)
    val day = args(5)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    val model = LogisticRegressionModel.load(modelPath)

    val pre_data = spark.sql(pre_sql)
      .map(r => {
        val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
        val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
        val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
        (
          r.getAs[String]("device"),
          r.getDouble(1),
          MlVectors.sparse(length, zipArray._1, zipArray._2)
        )
      }
      ).toDF("device", "label", "features")

    val data_training = model.setThreshold(threshold).transform(pre_data)

    data_training.select("device", "label", "prediction", "probability")
      .map(r =>
        (r.getString(0), r.getDouble(1), r.getDouble(2),
          r.getAs[org.apache.spark.ml.linalg.Vector](3).apply(r.getDouble(2).toInt))
      ).toDF("device", "label", "prediction", "probability")
      .repartition(10)
      .createOrReplaceTempView("lr_house_scoring")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = ${day}, kind = 'house')
         |select device,
         |if(label <> 1000.0, label, prediction) as prediction,
         |if(label <> 1000.0, 1.0, probability) as probability
         |from
         |lr_house_scoring
         |""".stripMargin)


  }
}