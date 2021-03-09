package com.youzu.ml
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}


object ALSTraining {


  def main(args: Array[String]) {

    if (args.length != 14) {
      System.err.println(
        "Usage: rankNum IterNum deviceFilePath pkgFilePath modelPath testDataPath" +
          " repartionNum sqlstmt lamda ImplicitPrefsFlag checkpointInterval checkpointPath"
      )
      System.exit(1)
    }

    val rankNum = Integer.parseInt(args(0))
    val IterNum = Integer.parseInt(args(1))
    val deviceFilePath = args(2)
    val pkgFilePath = args(3)
    val modelPath = args(4)
    val trainRepartionNum = Integer.parseInt(args(5))
    val testRepartitionNum = Integer.parseInt(args(6))
    val testDataSQL = args(7)
    val trianDataSQL = args(8)
    val trainDataForTestSQL = args(9)
    val lamda = args(10).toDouble
    val ImplicitPrefsFlag = args(11).toBoolean
    val checkpointInterval = Integer.parseInt(args(12))
    val checkpointPath = args(13)
    val sc = new SparkContext()

    sc.setCheckpointDir(checkpointPath)
    val sqlContext = new sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    deletePath(deviceFilePath, hdfs)
    deletePath(pkgFilePath, hdfs)
    deletePath(modelPath, hdfs)

    sqlContext.sql(trainDataForTestSQL).repartition(testRepartitionNum)
    sqlContext.sql(testDataSQL).repartition(testRepartitionNum)
    val temp_run_days_e_ind_training = sqlContext.sql(trianDataSQL).repartition(trainRepartionNum)

    val device_app_instal_rating = temp_run_days_e_ind_training.map { e =>
      val device_ind = e.get(0).toString.toInt
      val apppkg_ind = e.get(1).toString.toInt
      val weight = e.get(2).toString.toDouble
      Rating(device_ind, apppkg_ind, weight)
    }.cache()

    val numRatings = device_app_instal_rating.count()
    val numdevices = device_app_instal_rating.map(_.user).distinct().count()
    val numpkgs = device_app_instal_rating.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numdevices devices on $numpkgs pkgs.")

    device_app_instal_rating.unpersist(blocking = false)
    val model = new ALS().setRank(rankNum).setIterations(
      IterNum
    ).setCheckpointInterval(
      checkpointInterval
    ).setLambda(lamda).setImplicitPrefs(
      ImplicitPrefsFlag
    ).setUserBlocks(-1).setProductBlocks(-1).run(device_app_instal_rating)

    val deviceFile = model.userFeatures
    deviceFile.map {
      case (s, v) =>
        s.toString ++ "|" ++ v.mkString(",")
    }.saveAsTextFile(deviceFilePath)


    val pkgFile = model.productFeatures
    pkgFile.map { case (s, v) =>
      s.toString ++ "|" ++ v.mkString(",")
    }.saveAsTextFile(pkgFilePath)
    model.save(sc, modelPath)
    sc.stop()
  }

  def deletePath(path: String, hdfs: org.apache.hadoop.fs.FileSystem) {
    val output = new org.apache.hadoop.fs.Path(path)
    if (hdfs.exists(output)) hdfs.delete(output, true)
  }
}
