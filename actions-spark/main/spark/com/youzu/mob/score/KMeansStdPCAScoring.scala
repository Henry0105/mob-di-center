package com.youzu.mob.score

import org.apache.spark.SparkContext


import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.feature.PCA
import breeze.linalg.DenseMatrix
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import java.io.File
import breeze.linalg.csvwrite
import org.apache.spark.mllib.clustering.KMeansModel
import breeze.linalg.{Vector => BV, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import scala.io.Source

object KMeansStdPCAScoring extends Logging {
  def main(args: Array[String]) {

    println("length is :   " + args.length)
    for (i <- 0 until args.length)
      println(i + ": " + args(i))

    if (args.length != 8) {
      System.err.println("Usage: dailyData stdfile meanfile pcafile modelPath clusterLabelOutputPath")
      System.exit(1)
    }
    val sc = new SparkContext()
    val sqlContext = new sql.hive.HiveContext(sc)
    val deviceTfidfdata = args(0)

    val stdfile = sc.textFile(args(1))
    val meanfile = sc.textFile(args(2))
    val pcfile = sc.textFile(args(3))
    val modelPath = args(4)
    val clusterLabelOutput = args(5)

    val row_dim_num = Integer.parseInt(args(6))
    val col_dim_num = Integer.parseInt(args(7))
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    val output = new org.apache.hadoop.fs.Path(clusterLabelOutput)
    if (hdfs.exists(output)) hdfs.delete(output, true)
    val rawData = sqlContext.sql(deviceTfidfdata)

    val rdd3 = rawData.map { line =>
      val k = line.getString(0)
      val v = line.getString(1)
      val idx = v.split('=')(0).split(',').toArray.map(_.toInt)
      val value = v.split('=')(1).split(',').toArray.map(_.toDouble)
      (k, Vectors.sparse(row_dim_num, idx, value))
    }
    rdd3.cache

    val std = Vectors.dense(stdfile.toArray.map(_.toDouble))

    val mean = Vectors.dense(meanfile.toArray.map(_.toDouble))
    val scaler3 = new StandardScalerModel(std, mean)

    val t = pcfile.toArray.flatMap(_.split(",")).map(_.toDouble)

    val tmp = new DenseMatrix(row_dim_num, col_dim_num, t)
    val pcLoad = sc.broadcast(tmp)

    val km_mdl = KMeansModel.load(sc, modelPath)
    val km_scr = rdd3.map { case (s, v) =>
      val dv = scaler3.transform(v.toDense).toArray
      val bdv = BDV(dv)
      val rsl = bdv.toDenseMatrix * pcLoad.value
      val rs2 = km_mdl.predict(Vectors.dense(rsl.toArray))
      s ++ "|" ++ rs2.toString
    }

    km_scr.saveAsTextFile(clusterLabelOutput)
  }
}
