package com.youzu.mob.compress

import com.youzu.mob.compress.util.DouglasPeucker
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class Point(device: String, time: String, lat: Double, lon: Double)


object Compress {

  def main(args: Array[String]): Unit = {
    //
    val table_name = args(0)
    val outputTable = args(1)
    val tolerance = args(2).toDouble

    //    val outputTable = "test.hejy_compress_tmp_01"
    //    val tolerance = 0.001

    val spark = SparkSession
      .builder()
      .appName("compress")
      .enableHiveSupport()
      .getOrCreate()

    //    val rdd = spark.sql("")


    // 获取初始数据
    val rdd = spark.sql(
      s"""
         |select
         |device,time,lat,lon
         |from
         |${table_name}
         |where device is not null and lat is not null and lon is not null and time is not null
         |""".stripMargin)


    //    val rdd = spark.sql(
    //      s"""
    //         |select
    //         |device,time,lat,lon
    //         |from
    //         |dw_mobdi_md.tmp_data
    //         |where device is not null and lat is not null and lon is not null
    //         |""".stripMargin)

    import spark.implicits._

    rdd.rdd.mapPartitions(
      rows => {
        val results = ArrayBuffer[(String, Iterable[Point])]()
        rows.foreach(row => {
          val device = row.get(0).toString
          val time = row.get(1).toString
          val lat = row.get(2).toString.toDouble
          val lon = row.get(3).toString.toDouble
          results += ((device, Iterable(Point(device, time, lat, lon))))
        })
        results.toIterator
      }
    ).reduceByKey(_ ++ _).mapPartitions(
      rows => {
        val results = ArrayBuffer[(String, String, Double, Double)]()
        while (rows.hasNext) {
          val allPoint = rows.next
          val device = allPoint._1
          val point = allPoint._2
          val peucker = new DouglasPeucker()
          val points = peucker.compress(point.toArray, tolerance)
          results ++= points.map(a => (a.device, a.time, a.lat, a.lon))
        }
        results.toIterator
      }
    ).toDF("device", "time", "lat", "lon").createOrReplaceTempView("tmp_data")


    spark.sql(s"drop table if exists ${outputTable}")
    spark.sql(
      s"""
         |create table ${outputTable} as
         |select *
         |from
         |tmp_data
      """.stripMargin)

  }
}
