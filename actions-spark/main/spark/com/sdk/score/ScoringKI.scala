package com.sdk.score
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}
import scala.collection.mutable._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.RandomForestModel


object ScoringKI extends Logging {

  val sc = new SparkContext()
  val sqlContext = new sql.hive.HiveContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]) {

    if (args.length != 5) {
      logInfo("您调用main方法时参数有误！")
      println("您调用main方法时指定的参数包括：<DATE> <KidModelPath> <IncomeModelPath> <RawDataPath> <sparseDimNum>")
      return
    }

    val date = args(0)
    val kidsModelPath = args(1)
    val incomeModelPath = args(2)
    val rawDataPath = args(3)
    val sparseDimNum = Integer.parseInt(args(4))
    val toDate = new SimpleDateFormat("yyyyMMdd").parse(date)
    val lastDate = new SimpleDateFormat("yyyyMMdd").format(toDate.getTime - 1 * 24 * 60 * 60 * 1000)
    logInfo("DATE IS " + date)
    logInfo("LastDate Is " + lastDate)
    logInfo("KidsModelPath IS " + kidsModelPath)
    logInfo("IncomModelPath IS " + incomeModelPath)
    logInfo("RawDataPath IS " + rawDataPath)

    val rawData = sc.textFile(rawDataPath)

    val rawDataF = rawData
      .filter(r => (r.split('|')(5) != null && r.split('|')(5) != "" && r.split('|')(5) != "\\N"))
      .repartition(4000)
    rawDataF.cache()

    val RFKids = RandomForestModel.load(sc, kidsModelPath)
    val kids = rawDataF
      .map {
        line =>
          val str = line.split('|')
          val k = str(0)
          val v = ArrayBuffer[Double]()
          for (i <- 2 to 4) yield v += str(i).toDouble
          val v5 = str(5)
          val slice = Seq(0, 31, 133, 135, 136, 137, 178, 276, 303, 341, 370, 553, 562, 563, 564, 567)
          val idx = v5.split('=')(0).split(',').map(_.toInt)
          val value = v5.split('=')(1).split(',').map(_.toDouble)
          val tmp = Vectors.sparse(sparseDimNum, idx, value)
          val word_tl = for (i <- slice) yield if (tmp.apply(i) > 0) 1.0 else 0.0
          v ++= (word_tl.toArray)
          (k, v.toList)
      }.toDF("key", "feat")

    val dev_kids = kids.map(r => (r.get(0).toString, Vectors.dense(r.getAs[ArrayBuffer[Double]](1).toArray)))

    val KidsPrediction = dev_kids.map {
      r =>
        (r._1, if (RFKids.predict(r._2) == 0) {
          RFKids.predict(r._2)
        } else {
          RFKids.predict(r._2) - 1.0
        })
    }
    val DevKids = KidsPrediction.toDF("deviceid", "kids")
    DevKids.registerTempTable("kids_rdd_tmp")

    val RFIncome = RandomForestModel.load(sc, incomeModelPath)
    val income = rawDataF.map {
      line =>
        val str = line.split('|')
        val k = str(0)
        val v = ArrayBuffer[Double]()
        for (i <- 1 to 4) yield v += str(i).toDouble
        val v5 = str(5)
        val slice = Seq(
          33, 53, 64, 89, 90, 91, 147, 171, 176, 181, 182, 193, 206, 216, 218, 224,
          232, 233, 259, 272, 273, 274, 278, 279, 334, 335, 336, 337, 446, 458, 576, 577)
        val idx = v5.split('=')(0).split(',').map(_.toInt)
        val value = v5.split('=')(1).split(',').map(_.toDouble)
        val tmp = Vectors.sparse(sparseDimNum, idx, value)
        val word_tl = for (i <- slice) yield if (tmp.apply(i) > 0) 1.0 else 0.0
        v ++= (word_tl.toArray)
        (k, v.toList)
    }.toDF("key", "feat")

    val dev_income = income.map(
      r => (r.get(0).toString, Vectors.dense(r.getAs[ArrayBuffer[Double]](1).toArray)))
    val IncomePrediction = dev_income.map {
      r => (r._1, RFIncome.predict(r._2))
    }
    val DevIncome = IncomePrediction.toDF("deviceid", "income")
    DevIncome.registerTempTable("income_rdd_tmp")

    val lastUserScore = sqlContext
      .sql("select deviceid,kids,income,day from dm_sdk_mapping.device_user_score where day=" + lastDate).
      toDF("deviceid", "kids", "income", "day")

    val KI_DF = sqlContext.sql(
      "select x.deviceid,kids,income,'" + date +
        "' as day from kids_rdd_tmp x join income_rdd_tmp y on(x.deviceid=y.deviceid)")
      .toDF("deviceid", "kids", "income", "day")

    lastUserScore
      .unionAll(KI_DF)
      .toDF("deviceid", "kids", "income", "day")
      .distinct.registerTempTable("mid_results_tmp")

    sqlContext.sql(
      "select deviceid,kids,income,(" +
        "row_number() over (partition by deviceid order by day)) rn from mid_results_tmp")
      .toDF("deviceid", "kids", "income", "rn")
      .registerTempTable("results_tmp")
    //
    sqlContext.sql("select deviceid,kids,income from results_tmp where rn=1").registerTempTable("result_ki")
    val res_sql = "insert overwrite table dm_sdk_mapping.device_user_score partition(day='" + date + "') " +
      "select deviceid,max(gender) as gender,max(age) as age,max(edu) as edu,max(kids) as kids,max(income) as income " +
      "from(select COALESCE(x.deviceid,y.deviceid) as deviceid, " +
      "COALESCE(x.age,-1) as age,COALESCE(x.gender,-1) as gender" +
      ",COALESCE(x.edu,-1) as edu,COALESCE(y.kids,-1) as kids,COALESCE(y.income,-1) as income " +
      "from(select deviceid,age,gender,edu from dm_sdk_mapping.device_user_score where day=" + date + ")x " +
      "full outer join result_ki y on(x.deviceid=y.deviceid) " +
      ")xx " +
      "group by deviceid"

    sqlContext.sql(res_sql)
    sc.stop()
  }
}
