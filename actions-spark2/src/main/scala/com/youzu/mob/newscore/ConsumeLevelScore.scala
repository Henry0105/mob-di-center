package com.youzu.mob.newscore

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vectors => MlVectors}

import scala.collection.mutable.ArrayBuffer

object ConsumeLevelScore {

  def main(args: Array[String]): Unit = {

    println(args.mkString(","))
    val modelPath = args(0)
    val pre_sql = args(1)
    val thresholds = args(2).split(",").map(_.toDouble)
    val out_put_table = args(3)
    val day = args(4)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    val pre_data = spark.sql(pre_sql)
    val oneHot = new OneHotEncoderEstimator()
      .setInputCols(Array("city_level_1001", "factory", "sysver", "app_cnt", "price"))
      .setOutputCols(Array("city_level_1hot", "factory_1hot", "sysver_1hot", "app_cnt_1hot", "price_1hot"))

    val rawPred = pre_data.rdd.map { r =>
      val device = r.getString(0)
      val cityKey = r.getAs[Int]("city_level_1001").toDouble
      val factoryKey = r.getAs[Int]("factory").toDouble
      val sysverKey = r.getAs[Int]("sysver").toDouble
      val appCntKey = r.getAs[Int]("app_cnt").toDouble
      val priceKey = r.getAs[Int]("price").toDouble
      val cateKey = r.getAs[ArrayBuffer[Int]]("cate_index_list").toArray.filter(key => key != 0)
      val cateValue = r.getAs[ArrayBuffer[Double]]("cate_cnt_list").toArray.filter(value => value != 0.0)
      val appKey = r.getAs[ArrayBuffer[Int]]("apppkg_index_list").toArray.filter(key => key != 0)
      val appValue = List.fill(appKey.length)(1.0)

      val czip = cateKey.zip(cateValue).sortBy(_._1).unzip
      val azip = appKey.zip(appValue).sortBy(_._1).unzip

      (
        device,
        cityKey,
        factoryKey,
        sysverKey,
        appCntKey,
        priceKey,
        MlVectors.sparse(1000, czip._1, czip._2),
        MlVectors.sparse(350000, azip._1, azip._2)
      )
    }.toDF("device", "city_level_1001", "factory", "sysver", "app_cnt", "price", "cateFeature", "appFeature")

    val va = new VectorAssembler()
      .setInputCols(Array.concat(oneHot.getOutputCols, Array("cateFeature", "appFeature")))
      .setOutputCol("features")

    //管道操作
    val pipeline = new Pipeline().setStages(Array(oneHot, va))

    val predRDD = pipeline.fit(rawPred).transform(rawPred)

    val model = LogisticRegressionModel.load(modelPath)
    val data_training = model.setThresholds(thresholds).transform(predRDD)
    data_training.select("device", "prediction", "probability", "rawPrediction")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .repartition(10)
      .createOrReplaceTempView("lr_consume_level_scoring")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition(day = $day, kind = 'consume_level')
         |select device, prediction, probability
         |from lr_consume_level_scoring
         """.stripMargin)
  }
}
