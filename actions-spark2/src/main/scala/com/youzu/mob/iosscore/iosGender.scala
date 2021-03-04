package com.youzu.mob.iosscore

import com.youzu.mob.tools.SparkEnv
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.linalg.{Vector => mlVector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vector => mllibVector, Vectors => mllibVectors}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.ml.linalg.{Vector => MlVector, Vectors => MlVectors}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

class iosGender(@transient spark: SparkSession) {
  def score(args: Array[String]): Unit = {
    import spark.implicits._

    // input
    val day = args(0)
    val inputTable1 = args(1)
    val inputTable_chi = args(2)
    val outputTable = args(3)
    val modelPath = args(4)
    val threshold = args(5).toDouble
    val lrModel = LogisticRegressionModel.load(modelPath)

    var rdd_train2 = spark.sql(
      s"""
         |   select idfa, index, cnt
         |   from $inputTable_chi where day='$day'
  """.stripMargin)

    var rdd_train_2 = rdd_train2.map(r => (r.getString(0),
      org.apache.spark.ml.linalg.Vectors.dense(
        org.apache.spark.mllib.linalg.Vectors.sparse(4705,
          r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)
          .toArray
      ).toSparse)
    ).toDF("idfa", "feature2")

    var rdd_score = spark.sql(
      s"""
         |   select idfa, index, cnt
         |   from $inputTable1 where day='$day'
  """.stripMargin)

    var rdd_score_1 = rdd_score.map(r =>
      (r.getString(0),
        org.apache.spark.ml.linalg.Vectors.dense(
        org.apache.spark.mllib.linalg.Vectors.sparse(
          780, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)
          .toArray
        ).toSparse
      )
    ).toDF("idfa", "feature1")

    val joinData2 = rdd_score_1.join(rdd_train_2, Seq("idfa"), "inner")

    val scoreData = {
      new VectorAssembler()
        .setInputCols(joinData2.columns.slice(1, joinData2.columns.length))
        .setOutputCol("features")
        .transform(joinData2)
        .select("idfa", "features")
    }
    scoreData.cache()

    var predictions_lr = lrModel.setThreshold(threshold).transform(scoreData)
    predictions_lr.groupBy("prediction").count.orderBy("prediction").show

    predictions_lr.select("idfa","prediction","probability").map(r => (r.getString(0),r.getDouble(1),r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt))).toDF("idfa","prediction","probability").createOrReplaceTempView("tmpTable2")

    spark.sql(s"insert overwrite table $outputTable  select * from tmpTable2")

  }

  // 由于不需要我们训练，mapping表也不更新，所以这段代码不用，留着万一哪次需要我们训练了
  def chisq(args: Array[String]): Unit = {
    import spark.implicits._

    val day = args(0)
    val beforechi = args(1)
    val outputTable = args(2)
    //val beforechi = "test.liwj_iosgender_beforechi"
    var rdd_training = spark.sql(
      s"""
         |   select idfa,cast(gender as double) as tag, index, cnt
         |   from $beforechi
  """.stripMargin)

    var rdd_structural_data_sample: RDD[LabeledPoint] = rdd_training.map(r => LabeledPoint(r.getDouble(1),Vectors.sparse(25990,r.getAs[ArrayBuffer[Int]](2).toArray,r.getAs[ArrayBuffer[Double]](3).toArray).toDense.toSparse)).rdd
    var featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(rdd_structural_data_sample)

    case class resultset(index: Int,
                         degreesOfFreedom: Int,
                         statistic: Double,
                         pValue: Double)
    //var chiSq_result=(0 until featureTestResults.length).map(i => resultset(i,featureTestResults(i).degreesOfFreedom,featureTestResults(i).statistic,featureTestResults(i).pValue))
    var chiSq_result=(0 until featureTestResults.length).map(i => (i,featureTestResults(i).statistic,featureTestResults(i).pValue))
    var chiSq_df=chiSq_result.toDF("index","Statistics","pvalue").registerTempTable("chiSq")
    //spark.sql("drop table if exists test.liwj_risk_sample2_chisq5")
    //spark.sql("create table test.liwj_iosgender_chisq as select * from chiSq")

    spark.sql(
      s"""
         |insert overwrite table $outputTable
         |select index index_before
         |      ,row_number() over(order by pvalue,statistics desc) -1 as index_after
         |from chiSq
         |where pvalue <= 0.01
         |""".stripMargin)
  }
}

object iosGender {
  def main(args: Array[String]): Unit = {
    var day = args(0)


    val spark = SparkEnv.initial(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")

    import spark.implicits._

    //spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 1000)

    new iosGender(spark).score(args)
  }
}

object iosGenderChisq {
  def main(args: Array[String]): Unit = {
    var day = args(0)


    val spark = SparkEnv.initial(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", 1000)

    new iosGender(spark).chisq(args)
  }
}