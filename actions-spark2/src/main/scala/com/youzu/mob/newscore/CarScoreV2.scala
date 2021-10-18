package com.youzu.mob.newscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object CarScoreV2 {
  def main(args: Array[String]): Unit = {

    val partApp2vecTable = args(0)
    val part1Table = args(1)
    val part2Table = args(2)
    val part3Table = args(3)
    val part4Table = args(4)
    val part5Table = args(5)
    val part6Table = args(6)
    val part7Table = args(7)
    val part8Table = args(8)
    val modelPath = args(9)
    val threshold = args(10).split(",").map(_.toDouble)
    val outputTable = args(11)
    val day = args(12)


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    // 特征处理

    val rdd_train_1 = spark.sql(s"select device,index, cnt from $part1Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(288, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature1")

    val rdd_train_2 = spark.sql(s"select device,index, cnt from $part2Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(4273, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature2")

    val rdd_train_3 = spark.sql(s"select device,index, cnt from $part3Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(67, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature3")

    val rdd_train_4 = spark.sql(s"select device,index, cnt from $part4Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(46, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature4")

    val rdd_train_5 = spark.sql(s"select device,index, cnt from $part5Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(3015, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature5")

    val rdd_train_6 = spark.sql(s"select device,index, cnt from $part6Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(100, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature6")

    val rdd_train_7 = spark.sql(s"select device,index, cnt from $part7Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(3588, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature7")

    val rdd_train_8 = spark.sql(s"select device, index, cnt from $part8Table where day='$day'")
      .map(r => (r.getString(0),
        mlVectors.dense(mllibVectors.sparse(76, r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
      ).toDF("device", "feature8")

    val df_vec_base = spark.sql(s"select * from $partApp2vecTable where day='$day'").drop("day")

    val joinData_base =
      rdd_train_1
        .join(rdd_train_2, Seq("device"), "inner")
        .join(rdd_train_3, Seq("device"), "inner")
        .join(rdd_train_4, Seq("device"), "inner")
        .join(rdd_train_5, Seq("device"), "inner")
        .join(rdd_train_6, Seq("device"), "inner")
        .join(rdd_train_7, Seq("device"), "inner")
        .join(rdd_train_8, Seq("device"), "inner")
        .join(df_vec_base, Seq("device"), "inner")

    val trainData = {
      new VectorAssembler()
        .setInputCols(joinData_base.columns.slice(1, joinData_base.columns.length))
        .setOutputCol("features")
        .transform(joinData_base)
        .select("device", "features")
    }

    val lrModel = LogisticRegressionModel.load(modelPath)
    if (threshold.length > 1) {
      lrModel.setThresholds(threshold)
    } else {
      lrModel.setThreshold(threshold.head)
    }
    val predictions_lr = lrModel.transform(trainData)

    predictions_lr
      .select("device", "prediction", "probability")
      .map(r =>
        (
          r.getString(0),
          r.getDouble(1),
          r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)
        )
      )
      .toDF("device", "prediction", "probability")
      .repartition(2000)
      .createOrReplaceTempView("car_v2_lr_scoring")

    spark.sql(
      s"""
         |insert overwrite table $outputTable partition(day=$day,kind='car')
         |select device,prediction,probability
         |from car_v2_lr_scoring
         |""".stripMargin
    )
  }
}
