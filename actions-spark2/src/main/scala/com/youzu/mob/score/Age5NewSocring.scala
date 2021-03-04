package com.youzu.mob.score

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

/**
 * 年龄标签细化成5年一段是在原agebin（10年一段）结果的基础上进行预测的，所以要先score出agebin的结果，才能生成agebin_5的pre
 * Date: 2018/6/5
 * Time: 11:10
 * mvn:mvn package assembly:single
 *
 * @author zhtli
 */
object Age5NewSocring {

  def main(args: Array[String]): Unit = {
    println(args.mkString(","))
    if (args.length != 5) {
      println(
        s"""
           |ERROR:wrong number of parameters,new model of house need 5 parameters,
           |please check input parameters:
           |<day> <age_5_pre_sql> <model_path> <length> <mobid_model_par_table>
         """.stripMargin)
    }
    val day = args(0)
    val age_5_pre_sql = args(1)
    val modelpath = args(2)
    val length = args(3).toInt
    val mobid_model_par_table = args(4)

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_mobdi_profile_model_${day}")

    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._
    spark.sql(age_5_pre_sql).registerTempTable("mobdi_agebin_5_pre")

    var df_scoring_45_60 = spark.sql(
      "select device,index,cnt from mobdi_agebin_5_pre where agebin = 5")
    var df_structural_data_scoring_45_60 = df_scoring_45_60.map(
      r => (r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            length,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse)
    ).toDF("device", "features")

    var df_scoring_35_44 = spark.sql(
      "select device,index,cnt from mobdi_agebin_5_pre where agebin = 6")
    var df_structural_data_scoring_35_44 = df_scoring_35_44.map(
      r => (r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            length,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse)
    ).toDF("device", "features")

    var df_scoring_25_34 = spark.sql(
      "select device,index,cnt from mobdi_agebin_5_pre where agebin = 7")
    var df_structural_data_scoring_25_34 = df_scoring_25_34.map(
      r => (r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
          org.apache.spark.mllib.linalg.Vectors.sparse(
            length,
            r.getAs[ArrayBuffer[Int]](1).toArray,
            r.getAs[ArrayBuffer[Double]](2).toArray
          ).toArray
        ).toSparse)
    ).toDF("device", "features")


    var lrModel_45_60 = LogisticRegressionModel.load(modelpath + "/age_5_model_201805_45_60/")
    var lrModel_35_44 = LogisticRegressionModel.load(modelpath + "/age_5_model_201805_35_44/")
    var lrModel_25_34 = LogisticRegressionModel.load(modelpath + "/age_5_model_201805_25_34/")


    var predictions_45_60 = lrModel_45_60.
      setThresholds(Array(0.38, 0.42, 0.46, 0.5)).transform(df_structural_data_scoring_45_60)

    var predictions_35_44 = lrModel_35_44.
      setThresholds(Array(0.5, 0.5, 0.5, 0.5, 0.4, 0.5)).transform(df_structural_data_scoring_35_44)

    var predictions_25_34 = lrModel_25_34.
      setThresholds(Array(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.2, 0.5)).transform(df_structural_data_scoring_25_34)


    var predictions = predictions_45_60.union(predictions_35_44).union(predictions_25_34)

    predictions.select("device", "prediction", "probability")
      .map(r =>
          (r.getString(0), r.getDouble(1), r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt))
      )
      .toDF("device", "prediction", "probability")
      .registerTempTable("mobdi_lr_scoring_age_5")


    val agebin_5_all =
      s"""
         |INSERT OVERWRITE TABLE $mobid_model_par_table partition(day=$day, kind='agebin_1001')
         |select device,prediction,probability from (
         |select device,prediction,probability from mobdi_lr_scoring_age_5
         |union all
         |select device, case
         |when prediction= 9 then 9
         |when prediction = 8 then 8
         |else -1 end as prediction,probability
         |from $mobid_model_par_table where day=$day and kind='agebin' and prediction in (-1,8,9)
         |) a
         """.stripMargin

    println(agebin_5_all)

    spark.sql(agebin_5_all)

  }


}
