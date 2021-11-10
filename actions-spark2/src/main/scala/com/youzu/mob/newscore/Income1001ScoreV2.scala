package com.youzu.mob.newscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Income1001ScoreV2 {

  def main(args: Array[String]): Unit = {

    val part1Table = args(0)
    val part2Table = args(1)
    val part3Table = args(2)
    val part4Table = args(3)
    val part5Table = args(4)
    val part6Table = args(5)
    val part7Table = args(6)
    val part8Table = args(7)
    val partApp2vecTable = args(8)
    val modelPath = args(9)
    val threshold = args(10).split(",").map(_.toDouble)
    val output_table = args(11)
    val day = args(12)
    // val db = if (args(4)=="1") "mobdi_test" else "dw_mobdi_md"


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.udf.register("SVIndexStringUDF", (sv: org.apache.spark.mllib.linalg.Vector) => {
      sv match {
        case dv: DenseVector => dv.toSparse.indices.mkString(",")
        case sv: SparseVector => sv.indices.mkString(",")
      }
    })

    spark.udf.register("SVValueStringUDF", (sv: org.apache.spark.mllib.linalg.Vector) => {
      sv match {
        case dv: DenseVector => dv.toSparse.values.mkString(",")
        case sv: SparseVector => sv.values.mkString(",")
      }
    })


    var rdd_train1 = spark.sql(
      s"""
         |   select device,cast(0 as double) as tag, index, cnt
         |   from $part1Table
         |   where day='${day}'
       """.stripMargin)


    var rdd_train_1 = rdd_train1.map(r => (r.getString(0), r.getDouble(1),
       org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(288,
         r.getAs[ArrayBuffer[Int]](2).toArray, r.getAs[ArrayBuffer[Double]](3).toArray).toArray).toSparse)
    ).toDF("device", "tag", "feature1")


    var rdd_data2 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part2Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_2 = rdd_data2.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(37150,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature2")

    var rdd_data3 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part3Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_3 = rdd_data3.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(67,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature3")

    var rdd_data4 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part4Table
         |   where day='${day}'
       """.stripMargin)


    var rdd_train_4 = rdd_data4.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(46,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature4")


    val df_vec_base = spark.sql(
      s"""
         | select *
         | from $partApp2vecTable
         | where day='${day}'
       """.stripMargin).drop("day")

    var rdd_data5 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part5Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_5 = rdd_data5.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(20000,
         r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature5")

    var rdd_data6 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part6Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_6 = rdd_data6.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(100,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature6")

    var rdd_data7 = spark.sql(
      s"""
         |   select device,index, cnt
         |   from $part7Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_7 = rdd_data7.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(37150,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature7")

    var rdd_data8 = spark.sql(
      s"""
         |   select device,  index, cnt
         |   from $part8Table
         |   where day='${day}'
       """.stripMargin)

    var rdd_train_8 = rdd_data8.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(org.apache.spark.mllib.linalg.Vectors.sparse(50,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray).toSparse)
    ).toDF("device", "feature8")


    val joinData_base = {
      rdd_train_1
        .join(rdd_train_2, Seq("device"), "full_outer").na.fill({0})
        .join(rdd_train_3, Seq("device"), "full_outer").na.fill({0})
        .join(rdd_train_4, Seq("device"), "full_outer").na.fill({0})
        .join(df_vec_base, Seq("device"), "full_outer").na.fill({0})
        .join(rdd_train_5, Seq("device"), "full_outer").na.fill({0})
        .join(rdd_train_6, Seq("device"), "full_outer").na.fill({0})
        .join(rdd_train_7, Seq("device"), "full_outer").na.fill({0})
        .join(rdd_train_8, Seq("device"), "full_outer").na.fill({0})
    }



    val trainData = {
      new VectorAssembler()
        .setInputCols(joinData_base.columns.slice(2, joinData_base.columns.length))
        .setOutputCol("features")
        .setHandleInvalid("skip")
        .transform(joinData_base)
        .select("device", "tag", "features")
    }
    // trainData.cache()

    val saveTrainData = trainData.select("device", "tag", "features")


    import org.apache.spark.ml.linalg.Vectors
    // scoring===========
    var featureNum = 94951
    spark.udf.register("parseSparseVectorUDF", (index: Seq[Int], cnt: Seq[Double]) =>
      Vectors.sparse(
        featureNum,
        index.map(_.toInt)
          .zip(cnt.map(_.toDouble))
          .toSeq
      ))

    var lrModel = LogisticRegressionModel.load(modelPath)
    // 测试集
    // adjust
    lrModel.setThresholds(threshold)
    var predictions_lr = lrModel.transform(trainData)

    //复用
    predictions_lr.cache()

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
      .createOrReplaceTempView("income_lr_scoring")

    spark.sql(
      s"""
         |insert overwrite table ${output_table} partition(day=$day,kind='income_1001_v2')
         |select device,prediction+3.0 as prediction,probability
         |from income_lr_scoring
         |""".stripMargin
    )

    //输出所有probability
    predictions_lr
      .select("device", "probability")
      .map(r => (r.getString(0), r.getAs[org.apache.spark.ml.linalg.Vector](1).toArray))
      .toDF("device", "probability")
      .repartition(800)
      .createOrReplaceTempView("all_probability")

    spark.sql(
      s"""
         |insert overwrite table dw_mobdi_md.tmp_income_1001_all_probability partition(day = '$day')
         |select device,probability
         |from all_probability
         |""".stripMargin)


    spark.stop()

  }

}
