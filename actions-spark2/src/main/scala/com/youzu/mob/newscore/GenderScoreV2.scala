package com.youzu.mob.newscore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jpmml.evaluator.spark.TransformerBuilder
import org.jpmml.evaluator.{DefaultVisitorBattery, EvaluatorBuilder, LoadingModelEvaluatorBuilder}

/**
 * @author yuzhiyong
 * @date 2021/6/21 11:43
 *
 */
object GenderScoreV2 {
  def main(args: Array[String]): Unit = {
    // println(args.mkString(","))
    // 参数解析
    val day = args(0)
    val outputTable = args(1)
    val tmp_sql = args(2)
    val model_path = args(3)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport()
      .getOrCreate()

    val fs = FileSystem.get(new Configuration)
    val pmmlIs = fs.open(new Path(model_path))

    val evaluatorBuilder: EvaluatorBuilder = new LoadingModelEvaluatorBuilder()
      .setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs)

    val evaluator = evaluatorBuilder.build()
    val pmmlTransformerBuilder = new TransformerBuilder(evaluator)
      .withTargetCols()
      .withOutputCols()
      .exploded(true)

    // 获取特征数据
    var df: DataFrame = spark.sql(tmp_sql)

    // 数据过模型进行训练
    val df2: DataFrame = pmmlTransformerBuilder.build().transform(df)
      .withColumnRenamed("probability(0)", "p0")
      .withColumnRenamed("probability(1)", "p1")

    val resultDF: DataFrame = df2
    // 训练结果
    resultDF.select("p0", "p1", "device").createOrReplaceTempView("DF_table")

    spark.sql(
      s"""
        |select
        |  device,
        |  case when p0 >= p1 then 0 else 1 end as gender,
        |  case when p0 >= p1 then p0 else p1 end as probability
        |from DF_table
        |
        |""".stripMargin).createOrReplaceTempView("gender_score")

    // 导出数据
    spark.sql(
      s"""
        |insert overwrite table $outputTable partition(day=$day)
        |select
        |    device, gender, probability
        |from gender_score
        |""".stripMargin)

  }
}
