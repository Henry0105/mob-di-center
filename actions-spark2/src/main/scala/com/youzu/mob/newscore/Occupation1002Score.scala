package com.youzu.mob.newscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Occupation1002Score {
  def main(args: Array[String]): Unit = {


    val part1Table = args(0)
    val part2Table = args(1)
    val part3Table = args(2)
    val part4Table = args(3)
    val part5Table = args(4)
    val part6Table = args(5)
    val partApp2vecTable = args(6)
    val modelPath = args(7)
    val threshold = args(8).split(",").map(_.toDouble)
    val output_table = args(9)
    val day = args(10)
    // val db = if (args(11)=="test") "mobdi_test" else "dw_mobdi_md"


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    var rdd_data1 = spark.sql(
      s"""
         |select device, index, cnt
         |from $part1Table
         |where day='${day}'
       """.stripMargin)

    var rdd_train_1 = rdd_data1.map(r => (r.getString(0)
      , org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(288
      , r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature1")

    var rdd_data2 = spark.sql(
      s"""
         |select device,index, cnt
         |from $part2Table
         |where day='${day}'
       """.stripMargin)

    var rdd_train_2 = rdd_data2.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(88038
        , r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature2")


    var rdd_data3 = spark.sql(
      s"""
         |select device,index, cnt
         |from $part3Table
         |where day='${day}'
       """.stripMargin)

    var rdd_train_3 = rdd_data3.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(88
        , r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature3")

    var rdd_data4 = spark.sql(
      s"""
         |select device,index, cnt
         |from $part4Table
         |where day='${day}'
       """.stripMargin)

    var rdd_train_4 = rdd_data4.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(46
        , r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature4")


    val df_vec_base = spark.sql(
      s"""
         |select *
         |from $partApp2vecTable
         |where day='${day}'
       """.stripMargin).drop("day").na.fill(0)


    var rdd_data5 = spark.sql(
      s"""
         |select device,index, cnt
         |from $part5Table
         |where day='${day}'
       """.stripMargin)

    var rdd_train_5 = rdd_data5.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(20000
        , r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature5")

    var rdd_data6 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part6Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_6 = rdd_data6.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(100
        , r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature6")


    val joinData_base1 = {
      rdd_train_1
        .join(rdd_train_2, Seq("device"), "full_outer")
        .join(rdd_train_3, Seq("device"), "full_outer")
        .join(rdd_train_4, Seq("device"), "full_outer")
        .join(df_vec_base, Seq("device"), "full_outer")
        .join(rdd_train_5, Seq("device"), "full_outer")
        .join(rdd_train_6, Seq("device"), "full_outer")
    }

    val data_train = {
      new VectorAssembler()
        .setInputCols(joinData_base1.columns.slice(1, joinData_base1.columns.length))
        .setOutputCol("features")
        .setHandleInvalid("skip")
        .transform(joinData_base1)
        .select("device", "features")
    }

    // data_train.cache()

    /* data_train.createOrReplaceTempView("save_train_data")
    spark.sql("drop table if exists dw_mobdi_md.tmp_occ_pre_score_features")
    spark.sql("create table if not exists dw_mobdi_md.tmp_occ_pre_score_features as select * from save_train_data")

    spark.sql("drop table if exists dw_mobdi_md.tmp_occ_pre_score_features_idx_cnt")
    spark.sql("create table if not exists dw_mobdi_md.tmp_occ_pre_score_features_idx_cnt as " +
      "select device,features.indices as index,features.values as cnt " +
      "from dw_mobdi_md.tmp_occ_pre_score_features")

    // training===========
    import org.apache.spark.ml.linalg.Vectors
    val featureNum = 108660
    spark.udf.register("parseSparseVectorUDF", (index: Seq[Int], cnt: Seq[Double]) =>
      Vectors.sparse(
        featureNum,
        index.map(_.toInt)
          .zip(cnt.map(_.toDouble))
          .toSeq
      ))

    val test_data = spark.sql(
      s"""
         |   select device, index, cnt, parseSparseVectorUDF(index, cnt) as features
         |   from dw_mobdi_md.tmp_occ_pre_score_features_idx_cnt
      """.stripMargin) */
    // test_data.cache()

    // "hdfs://ShareSdkHadoop/user/tangzhzh/model202010/occ_lr"
    val lrModel = LogisticRegressionModel.load(modelPath)
    // Array(1.01, 1.2, 1.0, 1.02, 1.23, 1.22, 1.05, 1.3)
    lrModel.setThresholds(threshold)
    val predictions_lr = lrModel.transform(data_train)

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
      .createOrReplaceTempView("occ1002_lr_scoring")

    spark.sql(
      s"""
         |insert overwrite table ${output_table} partition(day=$day,kind='occupation_1002')
         |select device,prediction+13.0 as prediction,probability
         |from occ1002_lr_scoring
         |""".stripMargin
    )

  }
}
