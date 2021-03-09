package com.youzu.mob.label

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD
import java.util.Arrays.ArrayList
import org.apache.spark.sql.RowFactory
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import java.util.Arrays.ArrayList

object LBS_label {
  def main(args: Array[String]) {
    val day = args(0)
    val bday = args(1)
    val validtime = args(2)
    println("day=" + day + "|validtime=" + validtime)
    val sparkConf = new SparkConf().setAppName("LBS_label")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)


    val bsql1 = args(3)
    val bsql2 = args(4)
    val wsql1 = args(5)
    val wsql2 = args(6)
    val allsql = args(7)
    val partitions = args(8).toInt
    val filesnum = args(9).toInt

    hiveContext.sql(bsql1).registerTempTable("base_station_tmp1")
    hiveContext.sql(bsql2).registerTempTable("base_station_tmp2")
    hiveContext.sql(wsql1).registerTempTable("wifi_station_tmp1")
    hiveContext.sql(wsql2).registerTempTable("wifi_station_tmp2")
    val dfs = hiveContext.sql(allsql).distinct()
    val groupRows: RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])] =
      dfs.map(row => {
        val device = row.getAs[String]("device")
        val lat = row.getAs[String]("lat")
        val lon = row.getAs[String]("lon")
        val dtype = row.getAs[String]("type")
        val time = row.getAs[String]("time")
        val province = row.getAs[String]("province")
        val city = row.getAs[String]("city")
        val area = row.getAs[String]("area")
        val street = row.getAs[String]("street")
        val plat = row.getAs[String]("plat")
        (device, (lat, lon, dtype, time, province, city, area, street, plat))
      }).groupByKey()
    val re = groupRows.map(maprows => {
      val device = maprows._1
      val rows = maprows._2.toArray.sortBy(_._4).toIterator

      val outlist = new ArrayBuffer[
        (String, String, String, String, String, String, String, String, String, String, String)]
      var old: (String, String, String, String, String, String, String, String, String) = null
      while (rows.hasNext) {
        val r = rows.next()
        val lat = r._1
        val lon = r._2
        if (null == old) {
          old = r
        } else if ((!old._1.equals(lat) || !old._2.equals(lon)) && timeDiff(old._4, r._4) >= 60) {
          outlist += ((device, old._1, old._2, old._3, old._4, r._4, old._5, old._6, old._7, old._8, old._9))
          old = r
        }
        if (!rows.hasNext) {
          outlist += ((device, lat, lon, r._3, old._4, "23:59:59", r._5, r._6, r._7, r._8, r._9))
        }

      }
      outlist
    })
    val df = re.flatMap { r => r }
    hiveContext.createDataFrame(df).registerTempTable("tmp1")
    hiveContext.sql("cache table tmp_cache as select * from tmp1")
    hiveContext.sql("select * from tmp_cache").coalesce(filesnum).registerTempTable("tmp_info")

    hiveContext.sql("insert overwrite table dm_mobdi_master.sdk_lbs_daily partition(day=" + day + ") "
      + "select * from tmp_info ")
    sc.stop()
  }

  def timeDiff(beforeTime: String, afterTime: String): Long = {
    var time: Long = 0
    try {
      val fdf = FastDateFormat.getInstance("HH:mm:ss")
      val before = fdf.parse(beforeTime).getTime
      val after = fdf.parse(afterTime).getTime
      time = Math.abs(before - after) / 1000
    } catch {
      case ex: Exception => println(
        "java.text.ParseException: Unparseable date: beforeTime=>" + beforeTime + "|afterTime=>" + afterTime)
    }
    time
  }
}