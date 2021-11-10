package com.youzu.mob.newscore

import com.youzu.mob.tools.SparkEnv
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * 学历模型
 * Date: 2021-11-11
 *
 * @author luost
 */
class EduScoreV2(@transient spark: SparkSession) {

  def score(args: Array[String]): Unit = {

    import spark.implicits._
    val day = args(0)
    val modelPath = args(1)
    val threshold = args(2).split(",").map(_.toDouble)

    val part1_sql = args(3)
    val part2_sql = args(4)
    val part3_sql = args(5)
    val part4_sql = args(6)
    val part5_sql = args(7)
    val part6_sql = args(8)
    val part7_sql = args(9)
    val part8_sql = args(10)
    val app2vec_sql = args(11)

    val taget_table = args(12)

    var rdd_train1 = spark.sql(part1_sql)

    var rdd_train_1 = rdd_train1.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            288,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature1")

    var rdd_data2 = spark.sql(part2_sql)

    var rdd_train_2 = rdd_data2.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            76096,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature2")

    var rdd_data3 = spark.sql(part3_sql)

    var rdd_train_3 = rdd_data3.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            67,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature3")

    var rdd_data4 = spark.sql(part4_sql)

    var rdd_train_4 = rdd_data4.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            46,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature4")

    val df_vec_base = spark.sql(app2vec_sql).drop("day").na.fill(0)

    var rdd_data5 = spark.sql(part5_sql)

    var rdd_train_5 = rdd_data5.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            20000,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature5")

    var rdd_data6 = spark.sql(part6_sql)

    var rdd_train_6 = rdd_data6.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            100,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature6")

    var rdd_data7 = spark.sql(part7_sql)

    var rdd_train_7 = rdd_data7.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            76096,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature7")

    var rdd_data8 = spark.sql(part8_sql)

    var rdd_train_8 = rdd_data8.map(
      r => (
        r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            50,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse
      )
    ).toDF("device", "feature8")

    val joinData_base = {
      rdd_train_1
        .join(rdd_train_2, Seq("device"), "inner")
        .join(rdd_train_3, Seq("device"), "inner")
        .join(rdd_train_4, Seq("device"), "inner")
        .join(df_vec_base, Seq("device"), "inner").na.fill(0)
        .join(rdd_train_5, Seq("device"), "inner")
        .join(rdd_train_6, Seq("device"), "inner")
        .join(rdd_train_7, Seq("device"), "inner")
        .join(rdd_train_8, Seq("device"), "inner")
    }

    val trainData = {
      new VectorAssembler()
        .setInputCols(joinData_base.columns.slice(1, joinData_base.columns.length))
        .setOutputCol("features")
        .transform(joinData_base)
        .select("device", "features")
    }

    //训练
    var lrModel = LogisticRegressionModel.load(modelPath)
    lrModel.setThresholds(threshold)
    var predictions = lrModel.transform(trainData)

    //数据转换
    predictions
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
      .createOrReplaceTempView("lr_scoring")

    //输出
    spark.sql(
      s"""
         |insert overwrite table ${taget_table} partition(day=$day,kind = 'edu')
         |select device,
         |       case
         |         when prediction = 0 then 6
         |         when prediction = 1 then 7
         |         when prediction = 2 then 8
         |         when prediction = 3 then 9
         |         else prediction
         |       end as prediction,
         |       probability
         |from lr_scoring
         |""".stripMargin
    )

    spark.stop()

  }

}

object EduScoreV2 {

  def main(args: Array[String]): Unit = {
    val day = args(0)
    val spark = SparkEnv.initial(s"edu_score_v2_${day}")

    new EduScoreV2(spark).score(args)
  }
}
