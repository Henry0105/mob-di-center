package com.youzu.ml
import scala.collection.mutable._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.{ SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


object ALSApp {

  val sc = new SparkContext()
  val sqlContext = new sql.hive.HiveContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    if (args.length != 9) {
      System.err.println("Usage: " +
        "numTrainingPer numTestPer rankNum IterNum deviceFilePath pkgFilePath modelPath repartionNum sqlstm")
      System.exit(1)
    }
    val numTrainingPer = args(0).toDouble
    val numTestPer = args(1).toDouble
    val rankNum = Integer.parseInt(args(2))
    val IterNum = Integer.parseInt(args(3))
    val deviceFilePath = args(4)
    val pkgFilePath = args(5)
    val modelPath = args(6)
    val repartionNum = Integer.parseInt(args(7))
    val sqlstm = args(8)
    val temp_run_days_e_ind = sqlContext.sql(sqlstm).repartition(repartionNum)
    val device_app_instal_rating = temp_run_days_e_ind.map { e =>
      val device_ind = e.get(0).toString.toInt
      val apppkg_ind = e.get(1).toString.toInt
      Rating(device_ind, apppkg_ind, (1.0).toDouble)
    }.cache()

    val numRatings = device_app_instal_rating.count()
    val numdevices = device_app_instal_rating.
      map(_.user).distinct().count()
    val numpkgs = device_app_instal_rating.
      map(_.product).distinct().count()


    println(s"Got $numRatings ratings from $numdevices devices on $numpkgs pkgs.")

    val splits = device_app_instal_rating.randomSplit(Array(numTrainingPer, numTestPer))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()

    println(s"Training: $numTraining, test: $numTest.")

    device_app_instal_rating.unpersist(blocking = false)
    val model = new ALS().setRank(rankNum).setIterations(
      IterNum
    ).setLambda(1.0).setImplicitPrefs(true).setUserBlocks(
      -1
    ).setNonnegative(true).setProductBlocks(-1).run(training)

    val deviceFile = model.productFeatures
    val deviceFile_2 = deviceFile.map { case (s, v) =>
      s.toString ++ "|" ++ v.mkString(",")
    }

    deviceFile_2.saveAsTextFile(deviceFilePath)
    val pkgFile = model.userFeatures
    val pkgFile_2 = pkgFile.map { case (s, v) =>
      s.toString ++ "|" ++ v.mkString(",")
    }


    pkgFile_2.saveAsTextFile(pkgFilePath)
    model.save(sc, modelPath)

    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating],
    implicitPrefs: Boolean): Double = {

    def mapPredictedRating(r: Double): Double = {
      if (r >= 0.5) (1.0).toDouble
      else (0.0).toDouble
    }

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      val rslt = if (x.rating >= 0.5) (1.0).toDouble else (0.0).toDouble
      rslt
    }

    val rdd1 = predictionsAndRatings.filter { x => x >= 0.5 }.count()
    val rdd2 = predictionsAndRatings.filter { x => x < 0.5 }.count()
    (rdd1 / (rdd1 + rdd2))
  }


}
