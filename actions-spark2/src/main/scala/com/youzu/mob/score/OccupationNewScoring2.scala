package com.youzu.mob.score

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ArrayBuffer

object OccupationNewScoring2 {
  def main(args: Array[String]): Unit = {

    val modelPath = args(0) // 模型位置
    val part1Table = args(1) // part1 表
    val part2Table = args(2) // part2 表
    val part3Table = args(3) // part3 表
    val part4Table = args(4) // part4 表
    val app2vecTable = args(5) // 词向量表
    val outputTable = args(6) // 输出表
    val day = args(7)


    val spark = SparkSession.builder()
      .appName("occ_score")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val lrModel = LogisticRegressionModel.load(modelPath)

    lrModel.setThresholds(Array(0.8, 0.6, 0.7, 1.3, 1.2, 1.0, 1.5, 1.6))
    val rdd_score1 = spark.sql(
      s"""
         |   select device, index, cnt
         |   from $part1Table
  """.stripMargin)
    //      .sortWith((x, y) => x < y)
    val rdd_score_1 = rdd_score1.map(r => (r.getString(0),
      Vectors.dense(
        Vectors.sparse(288,
          r.getAs[ArrayBuffer[Int]](1).toArray.sortWith((x, y) => x < y),
          r.getAs[ArrayBuffer[Double]](2).toArray
        ).toArray
      ).toSparse)
    ).toDF("device", "feature1")

    var rdd_data2 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part2Table
  """.stripMargin)

    var rdd_score_2 = rdd_data2.map(r => (r.getString(0),
      Vectors.dense(
        Vectors.sparse(5939,
          r.getAs[ArrayBuffer[Int]](1).toArray.sortWith((x, y) => x < y),
          r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature2")


    var rdd_data3 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part3Table
  """.stripMargin)
    var rdd_score_3 = rdd_data3.map(r => (r.getString(0),
      Vectors.dense(
        Vectors.sparse(88,
          r.getAs[ArrayBuffer[Int]](1).toArray.sortWith((x, y) => x < y),
          r.getAs[ArrayBuffer[Double]](2).toArray
        ).toArray
      ).toSparse)
    ).toDF("device", "feature3")

    var rdd_data4 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part4Table
  """.stripMargin)
    var rdd_score_4 = rdd_data4.map(r => (r.getString(0),
      Vectors.dense(
        Vectors.sparse(46,
          r.getAs[ArrayBuffer[Int]](1).toArray.sortWith((x, y) => x < y),
          r.getAs[ArrayBuffer[Double]](2).toArray
        ).toArray
      ).toSparse)
    ).toDF("device", "feature4")

    val df_vec_base = spark.sql(
      s"""
         | select *
         | from $app2vecTable
""".stripMargin)
    val df_device = df_vec_base.map(_.getString(0)).distinct().toDF("device")

    val joinData_base_pre = {
      df_device
        .join(rdd_score_1, Seq("device"), "full")
        .join(rdd_score_2, Seq("device"), "full")
        .join(rdd_score_3, Seq("device"), "full")
        .join(rdd_score_4, Seq("device"), "full")
    }.map {
      r =>
        (
          r.getString(0),
          if (r.get(1) == null) Vectors.dense(Vectors.sparse(288, Array(1), Array(0.0)).toArray).toSparse
          else r.getAs[Vector](1).toSparse,
          if (r.get(2) == null) Vectors.dense(Vectors.sparse(5939, Array(1), Array(0.0)).toArray).toSparse
          else r.getAs[Vector](2).toSparse,
          if (r.get(3) == null) Vectors.dense(Vectors.sparse(88, Array(1), Array(0.0)).toArray).toSparse
          else r.getAs[Vector](3).toSparse,
          if (r.get(4) == null) Vectors.dense(Vectors.sparse(46, Array(1), Array(0.0)).toArray).toSparse
          else r.getAs[Vector](4).toSparse
        )
    }.toDF("device", "feature1", "feature2", "feature3", "feature4")

    val joinData_base = joinData_base_pre.join(df_vec_base, Seq("device"), "full").na.fill({
      0
    })
    joinData_base.cache()
    // full join

    val scoreData = {
      new VectorAssembler()
        .setInputCols(joinData_base.columns.slice(1, joinData_base.columns.length))
        .setOutputCol("features")
        .transform(joinData_base)
        .select("device", "features")
    }


    scoreData.cache()
    val score = scoreData.sample(0.001)

    var predictions_score = lrModel.transform(scoreData)

    predictions_score.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1), r.getAs[Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("tmpTable1")
    spark.sql(s"insert overwrite table $outputTable partition(day='$day') select * from tmpTable1")
  }
}