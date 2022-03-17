package com.youzu.mob.score


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jpmml.evaluator.spark.TransformerBuilder
import org.jpmml.evaluator.{DefaultVisitorBattery, LoadingModelEvaluatorBuilder}

/**
 * @author yanhw
 */
object IncomeMobeye {
  def main(args: Array[String]): Unit = {

    val Array(day, input, output) = args

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 加载特征数据
    val data = spark.sql(s"select * from $input where day='$day'").drop("day")
    // 加载pmml 文件
    val pmmlIs = this.getClass.getClassLoader.getResourceAsStream("Ridge.pmml")
    val evaluator = new LoadingModelEvaluatorBuilder()
      .setLocatable(false)
      .setVisitors(new DefaultVisitorBattery)
      .load(pmmlIs)
      .build()

    val pmmlTransformerBuilder = new TransformerBuilder(evaluator)
      .withTargetCols()
      .withOutputCols()
      .exploded(true)

    val df = pmmlTransformerBuilder.build().transform(data)
    df.createOrReplaceTempView("income_result")
    spark.sql(s"insert overwrite table $output partition(day='$day') select device,agebin_1004,income from income_result")

  }
}
