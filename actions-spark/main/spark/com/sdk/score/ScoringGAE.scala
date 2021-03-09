package com.sdk.score
import scala.collection.mutable._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.RandomForestModel
import java.util.regex.Pattern


object ScoringGAE extends Logging {

  val sc = new SparkContext()
  val sqlContext = new sql.hive.HiveContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]) {

    if (args.length != 6) {
      logInfo("您调用main方法时参数有误！")
      println("您调用main方法时指定的参数包括：" +
        "<DATE> <GenderModelPath> <AgeModelPath> <EduModelPath> <gender_age_dim> <edu_dim>")
      return
    }

    val date = args(0)
    val genderModelPath = args(1)
    val ageModelPath = args(2)
    val eduModelPath = args(3)
    val gender_age_dim = Integer.parseInt(args(4))
    val edu_dim = Integer.parseInt(args(5))

    logInfo("DATE IS " + date)
    logInfo("GenderModelPath IS " + genderModelPath)
    logInfo("AgeModelPath IS " + ageModelPath)
    logInfo("EduModelPath IS " + eduModelPath)
    val genderModel = LogisticRegressionModel.load(sc, genderModelPath)
    val temp_rdd_gender = sqlContext.sql(
      "SELECT deviceid,id_idx,cnt FROM tp_sdk_model.temp_rdd_gender where deviceid is not null and trim(deviceid)<>'' ")
    val test_gender = temp_rdd_gender.map(
      r => (r.get(0).toString,
        Vectors.sparse(gender_age_dim, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)))
    val predictionAndLabels_gender = test_gender.map {
      r => (r._1, (genderModel.predict(r._2)))
    }
    val results_gender = predictionAndLabels_gender.toDF("deviceid", "gender")
    results_gender.registerTempTable("gender_rdd_tmp")
    val birdayModel = LogisticRegressionModel.load(sc, ageModelPath)
    val temp_rdd_age = sqlContext.sql(
      "SELECT deviceid, id_idx, cnt FROM tp_sdk_model.temp_rdd_age where deviceid is not null and trim(deviceid)<>'' ")
    val test_age = temp_rdd_age.map(
      r => (r.get(0).toString, Vectors.sparse(gender_age_dim,
        r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)))

    val predictionAndLabels_age = test_age.map {
      r => (r._1, (birdayModel.predict(r._2)))
    }
    val results_age = predictionAndLabels_age.toDF("deviceid", "age")
    results_age.registerTempTable("age_rdd_tmp")

    val eduModel = LogisticRegressionModel.load(sc, eduModelPath)
    val temp_rdd_edu = sqlContext.sql(
      "SELECT deviceid,id_idx,cnt FROM tp_sdk_model.temp_rdd_edu where deviceid is not null and trim(deviceid)<>'' ")
    val test_edu = temp_rdd_edu.map(
      r => (r.get(0).toString,
        Vectors.sparse(edu_dim, r.getAs[ArrayBuffer[Int]](1).toArray, r.getAs[ArrayBuffer[Double]](2).toArray)))

    val predictionAndLabels_edu = test_edu.map {
      r => (r._1, (eduModel.predict(r._2)))
    }
    val results_edu = predictionAndLabels_edu.toDF("deviceid", "edu")
    results_edu.registerTempTable("edu_rdd_tmp")

    val results = sqlContext.sql(
        "select COALESCE(x.deviceid,y.deviceid,z.deviceid) as deviceid," +
        "COALESCE(x.gender,-1) as gender,COALESCE(y.age,-1) as age,COALESCE(z.edu,-1) as edu,'" + date + "' as day "
        + "from gender_rdd_tmp x full outer join age_rdd_tmp y "
        + "on (x.deviceid=y.deviceid) full outer join edu_rdd_tmp z "
        + "on(x.deviceid=z.deviceid)").distinct.registerTempTable("result_tmp")

    val results_final = sqlContext.sql("select deviceid,max(gender) as gender,max(age) as age,max(edu) as edu,day " +
      " from result_tmp group by deviceid,day").registerTempTable("result_final")

    sqlContext.sql("insert overwrite table dm_sdk_mapping.device_user_score partition(day='" + date + "') " +
      "select deviceid as deviceid,gender as gender" +
      ",age as age,edu as edu,'-10.0' as kids,'-10.0' as income from result_final")

    sc.stop()
  }
}
