package com.youzu.mob.score

import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.linalg.{Vector => mlVector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vector => mllibVector, Vectors => mllibVectors}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object HouseNewScoring {
  /**
   * 1.pre_ sql 放在外面传入
   * 2.model path 传入，并且model和data需要传入到开发的路径下
   * 3.model的配置和表在上线的时候一定需要创建
   * 4.刷历史数据
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    println(args.mkString(","))
    if (args.length != 5) {
      println(
        s"""
           |ERROR:wrong number of parameters,new model of house need 6 parameters,
           |please check input parameters:
           |<day>  <house_pre_sql> <model_path> <length> <mobid_model_par_table>
         """.stripMargin)
    }

    val day = args(0)
    val house_pre_sql = args(1)
    val modelpath = args(2)
    val length = args(3).toInt
    val mobid_model_par_table = args(4)

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_${day}")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import spark.implicits._
    spark.sql(house_pre_sql)
      .registerTempTable("mobdi_house_pre_table_tmp")




    println("model path:" + modelpath)

    val house_model = LogisticRegressionModel.load(modelpath)
    house_model.setThreshold(0.4)

    val rdd_house_full = spark.sql(
      "select device, index, cnt from mobdi_house_pre_table_tmp")
      .map(r =>
        (r.getString(0),
          mlVectors.dense(
            mllibVectors.sparse(
              length, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray).toArray
          ).toSparse
        )
      ).toDF("device", "features")

    house_model.transform(rdd_house_full)
      .select("device", "prediction", "probability")
      .map(
        r =>
          (r.getString(0), r.getDouble(1), r.getAs[mlVector](2).apply(r.getDouble(1).toInt))
      ).toDF("device", "prediction", "probability")
      .registerTempTable("mobdi_result_new_house_rdd_tmp")

    val house_score_out =
      s"""
         |INSERT OVERWRITE TABLE $mobid_model_par_table partition(day=$day, kind='house')
         |select device,prediction , probability from mobdi_result_new_house_rdd_tmp
       """.stripMargin
    spark.sql(house_score_out)
  }

}
