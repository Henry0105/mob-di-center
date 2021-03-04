package com.youzu.mob.location.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import com.youzu.mob.dbscan.{Dbscan, DbscanPonit, DbscanTools}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable.ArrayBuffer


object DbscanUtils {


  def centerPoint(dataFrame: DataFrame, spark: SparkSession, trainType: Int): DataFrame = {
    val schema = "device,lon,lat,cluster,centerlon,centerlat,distance,day,hour,weight"
    import spark.implicits._
    dataFrame.rdd.mapPartitions(rows => {
      val results = ArrayBuffer[(String, Iterable[DbscanPonit])]()
      rows.foreach(row => {
        val point = new DbscanPonit
        val device = row.getAs[Any]("device").toString
        point.lon = row.getAs[Any]("lon").toString.toDouble
        point.lat = row.getAs[Any]("lat").toString.toDouble
        point.hour = row.getAs[Any]("hour").toString
        point.day = row.getAs[Any]("day").toString
        point.weight = row.getAs[Any]("weight").toString.toDouble
        results += ((device, Iterable(point)))
      })
      results.toIterator
    }).reduceByKey(_ ++ _)
      .mapPartitions(rows => {
        val results = ArrayBuffer[(String, Double, Double, Int, Double, Double, Double, String, String, Double)]()
        while (rows.hasNext) {
          val rdd = rows.next()
          val device = rdd._1
          val points = rdd._2.toList
          val numPoints = points.length
          var isGeohash = false
          val minPoints: Int =
            if (numPoints > 3 && numPoints < 100) {
              3
            } else if (numPoints >= 100 && numPoints < 1000) {
              5
            } else {
              isGeohash = true
              50
            }
          if (numPoints > 3) {
            Dbscan.train(points.toArray, 0.2, minPoints, isGeohash, trainType)
            DbscanTools.centerPoint(points.toArray)
            val distance = distanceGPS
            results ++= points.map(a => (device, a.lon, a.lat, a.cluster, a.centerLon, a.centerLat,
              distance(a.lon, a.lat, a.centerLon, a.centerLat), a.day, a.hour, a.weight))
          }
        }
        results.toIterator
      }).toDF(schema.split(","): _*)
  }


  def distanceGPS: (Double, Double, Double, Double) => Double =
    (workLon: Double, workLat: Double, homeLon: Double, homeLat: Double) => {
      val pi = 3.1415926
      val r: Double = 6370996.81
      // a1、a2、b1、b2分别为上面数据的经纬度转换为弧度
      val a1 = workLon * pi / 180.0
      val a2 = workLat * pi / 180.0
      val b1 = homeLon * pi / 180.0
      val b2 = homeLat * pi / 180.0
      val t1: Double = Math.cos(a1) * Math.cos(a2) * Math.cos(b1) * Math.cos(b2)
      val t2: Double = Math.cos(a1) * Math.sin(a2) * Math.cos(b1) * Math.sin(b2)
      val t3: Double = Math.sin(a1) * Math.sin(b1)
      val distance = Math.acos(t1 + t2 + t3) * r
      distance
    }

  def getDayForWeek: String => Int = (date: String) => {
    val format = new SimpleDateFormat("yyyyMMdd")
    val c = Calendar.getInstance()
    c.setTime(format.parse(date))
    val day = c.get(Calendar.DAY_OF_WEEK)
    day match {
      case 1 => 7
      case _ => day - 1
    }

  }
}
