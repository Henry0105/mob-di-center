package com.youzu.mob.tools

import java.net.URI

import com.alibaba.fastjson.{JSON, JSONObject}
import com.youzu.mob.tools.OnlineUniversalTools.getBeforeTime
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import com.youzu.mob.utils.Constants._

object OfflineUniversalTools {
  def main(args: Array[String]): Unit = {
    val json = args(0)
    var day = ""
    var fileNum = 0
    var windowTime = 0
    var field = ""
    var lbstype = ""
    try {
      val ob = JSON.parseObject(json)
      day = ob.get("partition").toString
      fileNum = ob.get("fileNum").toString.toInt
      field = ob.get("field").toString
      lbstype = ob.get("lbstype").toString
      windowTime = ob.get("windowTime").toString.toInt
    } catch {
      case ex: Exception => throw new Exception(s"Json is not standardized!  Json=>${json}")
    }
    val bday = getBeforeTime(day, windowTime)

    val spark = SparkSession.builder().enableHiveSupport()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_${lbstype}_${day}").getOrCreate()

    try {
      if (!checkData(spark, day, windowTime, lbstype)) {
        throw new Exception()
      }
    } catch {
      case ex: Exception => throw new Exception(
        s"""There is data of table('dm_mobdi_master.sdk_lbs_daily_poi') in the time window is empty!
           |Please check the table!--->dm_mobdi_master.sdk_lbs_daily_poi(from ${day} to ${bday})
        """.stripMargin)
    }
    import spark.implicits._
    val dineinRdd = spark.sql(
      s"""
         |select device,poiinfo,day from (
         |select
         |device,
         |poiinfo,
         |day ,Row_number() over(partition by device,poiinfo,day order by device ) as rank
         |from $DWS_DEVICE_LBS_POI_ANDROID_SEC_DI
         |where day <=${day} and day > ${bday} and type= ${lbstype} ) mm where mm.rank =1
      """.stripMargin)
    val dineinRows = dineinRdd.rdd.mapPartitions(row => {
      var res = List[(String, List[JSONObject])]()
      while (row.hasNext) {
        val r = row.next();
        val device: String = r.getAs[String]("device")
        val poiInfo = r.getAs[String]("poiinfo")
        val day = r.getAs[String]("day")
        val json = JSON.parseObject(poiInfo)
        json.put("day", day)
        res.::=(device, List(json))
      }
      res.toIterator
    }).reduceByKey((x, y) => {
      x ++ y
    })
    val schemaString = "device,feature,cnt"
    dineinRows.mapPartitions(maps => {
      val rdds = new ArrayBuffer[(String, String, String)]
      while (maps.hasNext) {
        val map = maps.next()
        val device = map._1
        val poi = map._2
        val poiField = field.split(",")

        for (f <- poiField) {
          var tablefile = f
          if (f.contains(":")) {
            tablefile = f.split(":")(0)
          }
          val map1 = getMapByKey(poi, f)
          val value =
            tablefile match {
              case "type" | "location" | "facilities" | "cate1" =>
                wordFrequencySplit(map1)
              case _ =>
                wordFrequency(map1)
            }
          val feature = tablefile
          if (!"".equals(value)) {
            rdds += ((device, feature, value))
          }
        }
        rdds += ((device, "total", "total=" + poi.map(p => {
          p.get("day").toString
        }).distinct.length))
      }
      rdds.toIterator
    }).toDF(schemaString.split(","): _*).registerTempTable("offline_tmp")
    spark.sql(s"cache table offline_cache as select ${schemaString} from offline_tmp")
    spark.sql(s"select ${schemaString} from offline_cache").coalesce(fileNum).registerTempTable("offline_coalsece")
    spark.sql(
      s"""insert overwrite table $TIMEWINDOW_OFFLINE_PROFILE
         |partition (flag=${lbstype},day=${day},timewindow=${windowTime})
         |select ${schemaString} from offline_coalsece
       """.stripMargin)
  }

  def getMapByKey(arr: List[JSONObject], key: String): Array[(String, String)] = {
    val c = ArrayBuffer[(String, String)]()
    if (key.contains(":")) {
      val getkey = key.split(":")(0)
      var splitStr = key.split(":")(1)
      if (splitStr.equals("|")) {
        splitStr = ","
      }
      for (map <- arr) {
        val key2 = map.get(getkey).toString
        if (!"".equals(key2)) {
          val day = map.get("day").toString
          key2.split(splitStr).foreach(f => {
            c += ((f, day))
          })
        }
      }
    } else {
      for (map <- arr) {
        val key2 = map.get(key).toString
        if (!"".equals(key2)) {
          val day = map.get("day").toString
          c += ((key2, day))
        }
      }
    }
    c.toArray
  }

  def checkData(spark: SparkSession, day: String, windowTime: Int, lbstype: String): Boolean = {
    var isture: Boolean = true
    val conf: Configuration = new Configuration
    breakable {
      for (i <- 0 to (windowTime - 1)) {
        val bfday = getBeforeTime(day, i)
        val fs = FileSystem.get(conf)
        val count = fs.getContentSummary(
          new Path(s"/user/hive/warehouse/dm_mobdi_topic.db/dws_device_lbs_poi_android_sec_di/type=${lbstype}/day=${bfday}"))
        println(s"${bfday}--->" + count.getLength)
        if (count.getLength < 1048576) {
          isture = false
          break
        }
      }
    }

    isture
  }

  def wordFrequencySplit(array: Array[(String, String)]): String = {
    if (array.length > 0) {
      val r = array.flatMap(arr => {
        arr._1.split(",").flatMap(s => {
          s.split("，").map(ss => {
            (ss, arr._2)
          })
        })
      }).groupBy(_._1).map(f => {
        val name = f._1
        val cnt = f._2.distinct.length
        (name, cnt)
      })
      formatString(r)
    } else {
      ""
    }
  }

  def wordFrequency(array: Array[(String, String)]): String = {
    if (array.length > 0) {
      val r = array.groupBy(_._1).map(f => {
        val name = f._1
        val cnt = f._2.distinct.length
        (name, cnt)
      })
      formatString(r)
    } else {
      ""
    }
  }

  def formatString(map: Map[String, Int]): String = {
    val keys = mutable.ArrayBuffer[String]()
    val values = mutable.ArrayBuffer[Int]()
    var num = 0
    map.foreach { m =>
      keys.+=(m._1)
      values.+=(m._2)
    }
    keys.mkString(",") + "=" + values.mkString(",")
  }

}
