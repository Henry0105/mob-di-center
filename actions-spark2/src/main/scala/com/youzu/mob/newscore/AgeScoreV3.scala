package com.youzu.mob.newscore

import com.youzu.mob.tools.SparkEnv
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.ArrayBuffer

/**
 * 年龄模型v3
 * Date: 2020-12-09
 * @author guanyt
 */
class  AgeScoreV3(@transient spark: SparkSession) {
  def score(args: Array[String]): Unit = {
    import spark.implicits._
    val modelPath = args(0)
    val modelPath0 = args(1)
    val modelPath1 = args(2)
    val modelPath2 = args(3)
    val threshold = args(4).split(",").map(_.toDouble)
    val whitelist = args(5)
    val out_put_table = args(6)
    val day = args(7)
    val testdb = if (args(8)=="1") "mobdi_test" else "dm_mobdi_tmp"


    var rdd_val1 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part1
         |   where day='${day}'
      """.stripMargin)

    var rdd_val_1 = rdd_val1.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(
        org.apache.spark.mllib.linalg.Vectors.sparse(288,
          r.getAs[ArrayBuffer[Int]](1).toArray,
          r.getAs[ArrayBuffer[Double]](2).toArray)
          .toArray
      ).toSparse)
    ).toDF("device", "feature1")

    var rdd_data2 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part2_v3
         |   where day='${day}'
      """.stripMargin)

    var rdd_train_2 = rdd_data2.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(80571,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature2")


    var rdd_data3 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part3
         |   where day='${day}'
      """.stripMargin)
    var rdd_train_3 = rdd_data3.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(67,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature3")

    var rdd_data4 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part4
         |   where day='${day}'
      """.stripMargin)

    var rdd_train_4 = rdd_data4.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(46,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature4")

    val df_vec_base = spark.sql(
      s"""
         | select *
         | from ${testdb}.tmp_score_app2vec_v3
         | where day='${day}'
      """.stripMargin).drop("day").na.fill(0)

    var rdd_data5 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part5_v3
         |   where day='${day}'
      """.stripMargin)
    var rdd_train_5 = rdd_data5.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(20000,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature5")

    var rdd_data6 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part6
         |   where day='${day}'
      """.stripMargin)
    var rdd_train_6 = rdd_data6.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(100,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature6")

    var rdd_data7 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part7
         |   where day='${day}'
      """.stripMargin)
    var rdd_train_7 = rdd_data7.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(80571,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature7")

    var rdd_data8 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from ${testdb}.tmp_score_part8
         |   where day='${day}'
      """.stripMargin)
    var rdd_train_8 = rdd_data8.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(50,r.getAs[ArrayBuffer[Int]](1).toArray,r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature8")

    val joinData_base2 = {
      rdd_val_1
        .join(rdd_train_2, Seq("device"), "inner")
        .join(rdd_train_3, Seq("device"), "inner")
        .join(rdd_train_4, Seq("device"), "inner")
        .join(df_vec_base, Seq("device"), "inner").na.fill({0})
        .join(rdd_train_5, Seq("device"), "inner")
        .join(rdd_train_6, Seq("device"), "inner")
        .join(rdd_train_7, Seq("device"), "inner")
        .join(rdd_train_8, Seq("device"), "inner")
    }

    //joinData_base2.printSchema()
    joinData_base2.cache()
    //print(joinData_base2.columns.slice(1, joinData_base2.columns.length).mkString(","))

    val valData = {
      new VectorAssembler()
        .setInputCols(joinData_base2.columns.slice(1, joinData_base2.columns.length))
        .setOutputCol("features")
        .setHandleInvalid("skip")
        .transform(joinData_base2)
        .select("device","features")
    }

    //valData.cache()
    //println("数据集大小" + valData.count())

    var lrModel = LogisticRegressionModel.load(modelPath)
    lrModel.setThresholds(threshold)
    var predictions = lrModel.transform(valData)


    var whiteDF = spark.sql(s"select device from $whitelist")


    predictions = predictions.join(whiteDF, predictions("device") === whiteDF("device"), "left_outer").select(
      predictions("device")
      , predictions("prediction")
      , predictions("probability")
      , predictions("features")
      , whiteDF("device").alias("device2")
    )

    predictions.select("device", "prediction", "probability", "device2").map(r =>
      (
        r.getString(0)
        , if (r.get(3) != null | r.getDouble(1)==0) 0
      else r.getDouble(1)
        , if (r.get(3) != null) 1
      else r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)
      )
    ).toDF("device", "prediction", "probability").repartition(2000).createOrReplaceTempView("lr_scoring")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = $day, kind = 'agebin_1002')
         |select device,
         |case
         |when prediction = 0 then 5
         |when prediction = 1 then 6
         |when prediction = 2 then 7
         |when prediction = 3 then 8
         |when prediction = 4 then 9
         |else prediction
         |end as prediction,
         |probability
         |from lr_scoring
         |
         |""".stripMargin)
    // 下面是5年段模型

    // 45岁以上模型
    val valData0 = predictions.where("prediction = 0 or device2 is not null").select("device", "features")

    val lrModel0 = LogisticRegressionModel.load(modelPath0)
    var predictions0 = lrModel0.transform(valData0)

    /*
    var  evaluator_a = new MulticlassClassificationEvaluator()
      .setLabelCol("tag")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    var accuracy1 = evaluator_a.evaluate(predictions1)
    */
    //accuracy0: Double = 0.2816541353383459
    //accuracy0: Double = 0.25949939686369117

    predictions0.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_scoring0")


    // 35-44岁模型
    val valData1 = predictions.where("prediction=1 and device2 is null").select("device", "features")


    var lrModel1 = LogisticRegressionModel.load(modelPath1)
    var predictions1 = lrModel1.transform(valData1)

    /*
    var evaluator_a = new MulticlassClassificationEvaluator().setLabelCol("tag").setPredictionCol("prediction").setMetricName("accuracy")
    var accuracy2 = evaluator_a.evaluate(predictions2)

     */
    //accuracy1: Double = 0.2548992673992674
    //accuracy1: Double = 0.2467866323907455
    predictions1.select("device", "prediction", "probability").
      map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_scoring1")


    // 25-34岁模型
    val valData2=predictions.where("prediction=2 and device2 is null").select("device", "features")
    var lrModel2 = LogisticRegressionModel.load(modelPath2)

    var predictions2 = lrModel2.transform(valData2)

    /*
    var evaluator_a = new MulticlassClassificationEvaluator().setLabelCol("tag").setPredictionCol("prediction").setMetricName("accuracy")
    var accuracy3 = evaluator_a.evaluate(predictions3)

     */
    //accuracy2: Double = 0.4558139534883721
    //accuracy2: Double = 0.45781272337383844
    predictions2.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .createOrReplaceTempView("lr_scoring2")


    spark.sql(
      s"""
         |select device, prediction, probability
         |from $out_put_table
         |where (prediction = 8.0 or prediction = 9.0 or prediction = -1.0) and day = $day and kind = 'agebin_1002'
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
      .repartition(2000)
      .createOrReplaceTempView("lr_scoring_1001")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = $day, kind = 'agebin_1003')
         |select device, prediction, probability
         |from lr_scoring_1001
         |""".stripMargin)
  }
}

object AgeScoreV3 {

  def main(args: Array[String]): Unit = {
    val day = args(0)
    val spark = SparkEnv.initial(s"agescore_v3_${day}")

    new AgeScoreV3(spark).score(args)
  }
}
