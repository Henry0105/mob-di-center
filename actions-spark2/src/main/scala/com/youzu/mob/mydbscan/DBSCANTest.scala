package com.youzu.mob.mydbscan

import java.net.URL
import java.util.Date

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object DBSCANTest {

  def main(args: Array[String]) {
    val isGeohash = args(0).toBoolean
    val sql = args(1)
    val rept = args(2).toInt
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val parsedData = spark.sql(sql.trim)
    val s1 = new Date().getTime

    val points = parsedData.rdd.repartition(rept).mapPartitions(rdds => {
      val retn = ArrayBuffer[(String, Iterable[DbscanPonit])]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val point = new DbscanPonit
        point.lon = rdd.getAs[String]("lon").toDouble
        point.lat = rdd.getAs[String]("lat").toDouble
        retn += ((rdd.getAs[String]("device"), Iterable(point)))
      }
      retn.toIterator
    }).reduceByKey((x, y) => x ++ y)
    points.first()
    val e1 = new Date().getTime
    println(s"1--points-->${s1}|${e1}  time=" + (e1 - s1))

    val schemaString = "device,lon,lat,cluster"
    import spark.implicits._
    val s2 = new Date().getTime
    val rp = points.mapPartitions(rdds => {
      val reList = ArrayBuffer[(String, Double, Double, Int)]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val device = rdd._1
        val points = rdd._2.toArray
        Dbscan.train(points, 0.2, 50, isGeohash)
        DbscanTools.centerPoint(points)
        reList ++= points.map(a => (device, a.lon, a.lat, a.cluster))
      }
      reList.toIterator
    }).toDF(schemaString.split(","): _*)
    rp.first()
    val e2 = new Date().getTime
    println(s"1--points-->${s2}|${e2}  time=" + (e2 - s2))
    rp.registerTempTable("data_tmp")
    spark.sql("DROP TABLE IF EXISTS test.dbscan_test_return_xdzhang")
    spark.sql(
      """
        |create table test.dbscan_test_return_xdzhang stored as orc as
        |select device,lon,lat,cluster from data_tmp
      """.stripMargin)

  }
}
