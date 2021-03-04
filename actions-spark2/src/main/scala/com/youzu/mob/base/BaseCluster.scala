package com.youzu.mob.base

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import com.youzu.mob.mydbscan._

object BaseCluster {

  def main(args: Array[String]): Unit = {

    // 表名
    val tableNameInput = args(0)
    val tableNameOutput = args(1)


    val spark = SparkSession
      .builder()
      .appName("baseCluster")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val parsedData = spark.sql(
      s"""
         |select concat(lac,'+',cell,'+',mnc) as key,lat,lon
         |from $tableNameInput
         |where lac is not null and cell is not null
         |and mnc is not null and lat is not null
         |and lon is not null
  """.stripMargin)


    val schemaString = "key,lon,lat,cluster,centerLon,centerLat"
    import spark.implicits._

    parsedData.rdd.repartition(5000) .mapPartitions(
        rdds => {
        val retn = ArrayBuffer[(String, Iterable[DbscanPonit])]()
        while (rdds.hasNext) {
          val rdd = rdds.next()
          val point = new DbscanPonit
          point.lon = rdd.getAs[Double]("lon")
          point.lat = rdd.getAs[Double]("lat")
          retn += ((rdd.getAs[String]("key"), Iterable(point)))
        }
        retn.toIterator
      }).reduceByKey((x, y) => x ++ y).mapPartitions(rdds => {
        val reList = ArrayBuffer[(String, Double, Double, Int, Double, Double)]()
        while (rdds.hasNext) {
          val rdd = rdds.next()
          val key = rdd._1
          val points = rdd._2.toArray
          var points_length = points.length
          var cnt = 0
          val minPoints = cnt match {
            case cnt if (points_length < 100) => 3
            case cnt if (points_length >= 100 && points_length < 1000) => 5
            case cnt if (points_length >= 1000 && points_length < 5000) => 50
            case cnt if (points_length >= 5000 && points_length < 10000) => 200
          }
          Dbscan.train(points, 0.1, minPoints, true)
          DbscanTools.centerPoint(points)
          reList ++= points.map(a => (key, a.lon, a.lat, a.cluster, a.centerLon, a.centerLat))
        }
        reList.toIterator
      })
      .toDF(schemaString.split(","): _*)
      .createOrReplaceTempView("base_cluster_data")

    spark.sql(
      s"""
         |insert overwrite table $tableNameOutput
         |select key, lon, lat, cluster, centerLon, centerLat
         |from base_cluster_data
  """.stripMargin).collect()

  }
}
