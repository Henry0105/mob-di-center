package com.youzu.mob.mydbscan

import java.net.URL
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

object DBSCAN_gps_cnt3 {

  def main(args: Array[String]) {

    if (args.length != 3) {
      println(
        s"""
           |error number of input parameters,please check your input
           |parameters like:<cnt3_data_pre_sql> <out_put_table> <day>
         """.stripMargin)
      System.exit(-1)
    }
    println(args.mkString(","))

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.classesToRegister", "com.youzu.mob.mydbscan.DbscanPonit")
      .getOrCreate()

    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())

    val gps_cnt3_pre = args(0)
    val out_put_table = args(1)
    val day = args(2)

    val parsedData = spark.sql(gps_cnt3_pre)

    val points = parsedData.rdd.repartition(5000).mapPartitions(rdds => {
      val retn = ArrayBuffer[(String, Iterable[DbscanPonit])]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val point = new DbscanPonit
        point.lon = rdd.getAs[Double]("lon")
        point.lat = rdd.getAs[Double]("lat")
        retn += ((rdd.getAs[String]("bssid"), Iterable(point)))
      }
      retn.toIterator
    }).reduceByKey((x, y) => x ++ y)

    val schemaString = "bssid,lon,lat,cluster,centerLon,centerLat"

    import spark.implicits._

    val rp = points.mapPartitions(rdds => {
      val reList = ArrayBuffer[(String, Double, Double, Int, Double, Double)]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val bssid = rdd._1
        val points = rdd._2.toArray
        val points_length = points.length
        val cnt = 0
        val minPoints = cnt match {
          case cnt if (points_length < 100) => 3
          case cnt if (points_length >= 100 && points_length < 1000) => 5
          case cnt if (points_length >= 1000 && points_length < 5000) => 50
          case cnt if (points_length >= 5000 && points_length < 10000) => 200
        }
        Dbscan.train(points, 0.1, minPoints, true)
        DbscanTools.centerPoint(points)
        reList ++= points.map(a => (bssid, a.lon, a.lat, a.cluster, a.centerLon, a.centerLat))
      }
      reList.toIterator
    }).toDF(schemaString.split(","): _*)

    rp.createOrReplaceTempView("data_tmp")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition(dt=$day)
         |select bssid, lon, lat, cluster, centerLon, centerLat
         |from data_tmp
      """.stripMargin)

  }
}
