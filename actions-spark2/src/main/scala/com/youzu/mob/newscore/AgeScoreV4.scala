package com.youzu.mob.newscore

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jpmml.evaluator.{DefaultVisitorBattery, LoadingModelEvaluatorBuilder}
import org.jpmml.evaluator.spark.TransformerBuilder

/**
 * @author yuzhiyong
 */
object AgeScoreV4 {
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val inputTable = args(1)
    val outputTable = args(2)
    val pre18 = args(3)
    val pre18_24 = args(4)
    val pre25_34 = args(5)
    val pre35_44 = args(6)
    val pre45_54 = args(7)
    val pre55 = args(8)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport()
      .getOrCreate()

    // 读取PMML文件
    val pmmlIs1 = this.getClass.getClassLoader.getResourceAsStream("age_below_18.pmml")
    val pmmlIs2 = this.getClass.getClassLoader.getResourceAsStream("age_between_18_24.pmml")
    val pmmlIs3 = this.getClass.getClassLoader.getResourceAsStream("age_between_25_34.pmml")
    val pmmlIs4 = this.getClass.getClassLoader.getResourceAsStream("age_between_35_44.pmml")
    val pmmlIs5 = this.getClass.getClassLoader.getResourceAsStream("age_between_45_54.pmml")
    val pmmlIs6 = this.getClass.getClassLoader.getResourceAsStream("age_beyond_55.pmml")

    // 构造pmml模型
    val evaluatorBuilder1 = new LoadingModelEvaluatorBuilder().setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs1)
    val evaluator1 = evaluatorBuilder1.build()
    val pmmlTransformerBuilder1 = new TransformerBuilder(evaluator1).withTargetCols()
      .withOutputCols().exploded(true)

    val evaluatorBuilder2 = new LoadingModelEvaluatorBuilder().setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs2)
    val evaluator2 = evaluatorBuilder2.build()
    val pmmlTransformerBuilder2 = new TransformerBuilder(evaluator2).withTargetCols()
      .withOutputCols().exploded(true)

    val evaluatorBuilder3 = new LoadingModelEvaluatorBuilder().setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs3)
    val evaluator3 = evaluatorBuilder3.build()
    val pmmlTransformerBuilder3 = new TransformerBuilder(evaluator3).withTargetCols()
      .withOutputCols().exploded(true)

    val evaluatorBuilder4 = new LoadingModelEvaluatorBuilder().setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs4)
    val evaluator4 = evaluatorBuilder4.build()
    val pmmlTransformerBuilder4 = new TransformerBuilder(evaluator4).withTargetCols()
      .withOutputCols().exploded(true)

    val evaluatorBuilder5 = new LoadingModelEvaluatorBuilder().setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs5)
    val evaluator5 = evaluatorBuilder5.build()
    val pmmlTransformerBuilder5 = new TransformerBuilder(evaluator5).withTargetCols()
      .withOutputCols().exploded(true)

    val evaluatorBuilder6 = new LoadingModelEvaluatorBuilder().setLocatable(false)
      .setVisitors(new DefaultVisitorBattery()).load(pmmlIs6)
    val evaluator6 = evaluatorBuilder6.build()
    val pmmlTransformerBuilder6 = new TransformerBuilder(evaluator6).withTargetCols()
      .withOutputCols().exploded(true)


    // 获取特征数据,必须对数据进行cache，不然影响后面的join
    val df: DataFrame = spark.sql(inputTable).cache()

    // 数据过模型进行训练
    // 第一个模型
    val df1: DataFrame = pmmlTransformerBuilder1.build().transform(df)
      .withColumnRenamed("probability(1)", "p1")

    df1.createOrReplaceTempView("device_score_18")

    val result1 = spark.sql(
      s"""
         |select * from
         |    (select * from device_score_18)a
         |    cross join
         |    (select bin_min as bin_min1,bin_max_test as bin_max_test1 ,precision_bin as precision1 from $pre18)b
         |    where a.p1 >= b.bin_min1 and a.p1 < b.bin_max_test1
         |""".stripMargin)

    // 第二个模型
    val df2: DataFrame = pmmlTransformerBuilder2.build().transform(result1)
      .withColumnRenamed("probability(1)", "p2")

    df2.createOrReplaceTempView("device_score_18_24")


    val result2 = spark.sql(
      s"""
         |select * from
         |    (select * from device_score_18_24)a
         |    cross join
         |    (select bin_min as bin_min2,bin_max_test as bin_max_test2 ,precision_bin as precision2 from $pre18_24)b
         |    where a.p2 >= b.bin_min2 and a.p2 < b.bin_max_test2
         |""".stripMargin)


    // 第三个模型
    val df3: DataFrame = pmmlTransformerBuilder3.build().transform(result2)
      .withColumnRenamed("probability(1)", "p3")

    df3.createOrReplaceTempView("device_score_25_34")



    val result3 = spark.sql(
      s"""
         |select * from
         |    (select * from device_score_25_34)a
         |    cross join
         |    (select bin_min as bin_min3,bin_max_test as bin_max_test3 ,precision_bin as precision3 from $pre25_34)b
         |    where a.p3 >= b.bin_min3 and a.p3 < b.bin_max_test3
         |""".stripMargin)


    // 第四个模型
    val df4: DataFrame = pmmlTransformerBuilder4.build().transform(result3)
      .withColumnRenamed("probability(1)", "p4")

    df4.createOrReplaceTempView("device_score_35_44")


    val result4 = spark.sql(
      s"""
         |select * from
         |    (select * from device_score_35_44)a
         |    cross join
         |    (select bin_min as bin_min4,bin_max_test as bin_max_test4 ,precision_bin as precision4 from $pre35_44)b
         |    where a.p4 >= b.bin_min4 and a.p4 < b.bin_max_test4
         |""".stripMargin)



    // 第五个模型
    val df5: DataFrame = pmmlTransformerBuilder5.build().transform(result4)
      .withColumnRenamed("probability(1)", "p5")

    df5.createOrReplaceTempView("device_score_45_54")


    val result5 = spark.sql(
      s"""
         |select * from
         |    (select * from device_score_45_54)a
         |    cross join
         |    (select bin_min as bin_min5,bin_max_test as bin_max_test5 ,precision_bin as precision5 from $pre45_54)b
         |    where a.p5 >= b.bin_min5 and a.p5 < b.bin_max_test5
         |""".stripMargin)


    // 第六个模型
    val df6: DataFrame = pmmlTransformerBuilder6.build().transform(result5)
      .withColumnRenamed("probability(1)", "p6")

    df6.createOrReplaceTempView("device_score_55")


    val result6 = spark.sql(
      s"""
        |select device,p1,precision1,p2,precision2,p3,precision3,p4,precision4,p5,precision5,p6,precision6 from
        |    (select * from device_score_55)a
        |    cross join
        |    (select bin_min bin_min6,bin_max_test bin_max_test6,precision_bin as precision6 from $pre55)b
        |    where a.p6 >= b.bin_min6 and a.p6 < b.bin_max_test6
        |""".stripMargin)

    result6.createOrReplaceTempView("result")
    // 生成最后的结果数据
    // device,p1,pre1,p2,pre2,p3,pre3,p4,pre4,p5,pre5,p6,pre6,label
    // 得出精确度最高的值和label

    spark.sql(""" select * ,
                |(case when max = precision1*0.4 then p1
                |when max = precision2*1.2 then p2 when max = precision3 then p3
                |when max = precision4 then p4 when max = precision5 then p5
                |else p6 end)as probability,
                |(case when max = precision1*0.4 then 0
                | when max = precision2*1.2 then 1 when max = precision3 then 2
                |when max = precision4 then 3 when max = precision5 then 4
                |else 5 end) as label
                |from (select *,greatest(precision1*0.4,precision2*1.2,precision3,precision4,precision5,precision6) max
                |from result)a""".stripMargin).createOrReplaceTempView("finalResult")


    // 将结果数据的导入到hive表中

    spark.sql(
      s"""
         |insert overwrite table $outputTable partition(day = $day)
         |select * from finalResult
         |""".stripMargin)

  }

}
