package com.youzu.mob.newscore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jpmml.evaluator.spark.TransformerBuilder
import org.jpmml.evaluator.{DefaultVisitorBattery, LoadingModelEvaluatorBuilder}

object IncomeScoreV2 {

  def main(args: Array[String]): Unit = {

    println(args.mkString(","))
    val input_table=args(0)
    val model_path1=args(1)
    val model_path2=args(2)
    val model_path3=args(3)
    val model_path4=args(4)
    val model_path5=args(5)
    val model_path1_pre=args(6)
    val model_path2_pre=args(7)
    val model_path3_pre=args(8)
    val model_path4_pre=args(9)
    val model_path5_pre=args(10)
    val output_table=args(11)
    val day=args(12)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    val fs = FileSystem.get(new Configuration())
    // 加载特征数据
    val data = spark.sql(s"select * from ${input_table}")

    // 3k以下二分类模型
    getResult(spark, fs, data, model_path1, model_path1_pre).createOrReplaceTempView("result_1")
    // 3k-5k二分类模型
    getResult(spark, fs, data, model_path2, model_path2_pre).createOrReplaceTempView("result_2")
    // 5k-10k二分类模型
    getResult(spark, fs, data, model_path3, model_path3_pre).createOrReplaceTempView("result_3")
    // 10-20k以下二分类模型
    getResult2(spark, fs, data, model_path4, model_path4_pre).createOrReplaceTempView("result_4")
    // 20k以上二分类模型
    getResult2(spark, fs, data, model_path5, model_path5_pre).createOrReplaceTempView("result_5")

    // 初步合并总的模型分
    val result_all = spark.sql(
      """
        | select
        |     a.device,
        |     a.prob as prob1,
        |     a.precision_bin as precision_bin1,
        |     b.prob as prob2,
        |     b.precision_bin as precision_bin2,
        |     c.prob as prob3,
        |     c.precision_bin as precision_bin3,
        |     d.prob as prob4,
        |     d.precision_bin as precision_bin4,
        |     e.prob as prob5,
        |     e.precision_bin as precision_bin5
        | from
        | (  select device,prob,precision_bin from result_1  ) a
        | inner join
        | (  select device,prob,precision_bin from result_2  ) b
        | on a.device = b.device
        | inner join
        | (  select device,prob,precision_bin from result_3  ) c
        | on a.device = c.device
        | inner join
        | (  select device,prob,precision_bin from result_4  ) d
        | on a.device = d.device
        | inner join
        | (  select device,prob,precision_bin from result_5  ) e
        | on a.device = e.device
        |""".stripMargin)

    println("======================= result_all.printSchema() ================================")
    result_all.printSchema()


    // 注册udf
    val maxLabelAsMapUDF = udf((p1: Double, p2: Double,p3: Double,p4: Double,p5: Double) => {
      val nums: List[Double] = List(p1, p2, p3, p4,p5)
      var index=0
      var max_p=0.0
      for (i <-0 until 5){
        if(nums(i)>max_p){
          index = i
          max_p = nums(i)
        }
      }
      (index,max_p)
    })

    val finalDf = result_all.withColumn("label_res", maxLabelAsMapUDF(col("precision_bin1"),col("precision_bin2"),col("precision_bin3"), col("precision_bin4"),col("precision_bin5")))
    var finalDf1 = finalDf.withColumn("label",$"label_res".getItem("_1"))
    finalDf1 = finalDf1.withColumn("max_prob",$"label_res".getItem("_2"))
    finalDf1.createOrReplaceTempView("res_final")

    println("======================= res_final.printSchema() ================================")
    finalDf1.printSchema()


    // 将结果写入库表
    spark.sql(
      s"""
         |insert overwrite table $output_table partition (day='${day}')
         |select device,prob1,precision_bin1,prob2,precision_bin2,prob3,precision_bin3,prob4,precision_bin4,prob5,precision_bin5,label_res,
         |(case
         |    when max = precision_bin1*0.6 then 0
         |    when max = precision_bin2*0.8 then 1
         |    when max = precision_bin3*1.1 then 2
         |    when max = precision_bin4*1.25 then 3
         |    when max = precision_bin5*0.9 then 4
         |    else 5
         |end) as label,
         |(case
         |    when max = precision_bin1*0.6 then precision_bin1
         |    when max = precision_bin2*0.8 then precision_bin2
         |    when max = precision_bin3*1.1 then precision_bin3
         |    when max = precision_bin4*1.25 then precision_bin4
         |    when max = precision_bin5*0.9 then precision_bin5
         |    else 5
         |end) as max_prob
         |from
         |(
         |    select *, greatest(precision_bin1*0.6,precision_bin2*0.8,precision_bin3*1.1,precision_bin4*1.25,precision_bin5*0.9) max
         |    from res_final
         |) a
         |""".stripMargin)

    println("======================= 入库完成 ================================")

    // 关闭spark
    spark.stop()

  }

  /**
   * @param model_path  模型文件
   * @return
   */
  def createTransformer(fs: FileSystem, model_path: String): Transformer = {
    // 加载pmml 文件
    val pmml= fs.open(new Path(model_path))
    val evaluator = new LoadingModelEvaluatorBuilder().setLocatable(false).setVisitors(new DefaultVisitorBattery).load(pmml).build()
    val transformerBuilder = new TransformerBuilder(evaluator).withTargetCols().withOutputCols().exploded(true)
    transformerBuilder.build()
  }

  /**
   * 生成对应的结果表
   * @param spark
   * @param fs
   * @param data            特征数据
   * @param model_path      模型文件
   * @param model_path_pre  映射文件
   * @return
   */
  def getResult(spark: SparkSession, fs: FileSystem, data: DataFrame, model_path: String, model_path_pre: String): DataFrame = {
    createTransformer(fs, model_path).transform(data).select("device","probability(1)").createOrReplaceTempView("device_score_tmp")
    // 加载分数映射文件
    spark.read.option("header",true).option("delimiter", ",").csv(model_path_pre).orderBy("bin_max_test").createOrReplaceTempView("score_bins_tmp")
    // 生成 result_1
    val result: DataFrame = spark.sql(
      """
        |select device,prob,precision_bin
        |from
        |( select device,`probability(1)` as prob from device_score_tmp )a
        |cross join
        |( select bin_min,bin_max_test,precision_bin from score_bins_tmp )b
        |where a.prob>=b.bin_min and a.prob<b.bin_max_test
        |""".stripMargin)
    result
  }


  /**
   * 生成对应的结果表
   * @param spark
   * @param fs
   * @param data            特征数据
   * @param model_path      模型文件
   * @param model_path_pre  映射文件
   * @return
   */
  def getResult2(spark: SparkSession, fs: FileSystem, data: DataFrame, model_path: String, model_path_pre: String): DataFrame = {
    createTransformer(fs, model_path).transform(data).select("device","probability(1)").createOrReplaceTempView("device_score_tmp")
    // 加载分数映射文件
    spark.read.option("header",true).option("delimiter", ",").csv(model_path_pre).orderBy("bin_max_test").createOrReplaceTempView("score_bins_tmp")
    // 生成 result_1
    val result: DataFrame = spark.sql(
      """
        |select device,prob, precison_bin as precision_bin
        |from
        |( select device,`probability(1)` as prob from device_score_tmp )a
        |cross join
        |( select bin_min,bin_max_test,precison_bin from score_bins_tmp )b
        |where a.prob>=b.bin_min and a.prob<b.bin_max_test
        |""".stripMargin)
    result
  }
}
