package com.youzu.mob.label

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.{JSON, JSONObject}

object Timewindow_offline {
  def main(args: Array[String]) {
    val parTime = args(0)
    val sdk_poi_sql = args(1)
    val field = args(2)
    val fileNum = args(3).toInt
    val URL = args(4)
    val lbstype = args(5)
    val dayNum = args(6)
    val sparkConf = new SparkConf().
      setAppName("Spark Application " + this.getClass.getSimpleName.stripSuffix("$") + s"_${parTime}")
    val sc = new SparkContext(sparkConf)
    val spark = new HiveContext(sc)
    import spark.implicits._

    val dineinRdd = spark.sql(sdk_poi_sql)
    val dineinRows = dineinRdd.mapPartitions(row => {
      var res = List[(String, List[JSONObject])]()
      while (row.hasNext) {
        val r = row.next();
        val device: String = r.getAs[String]("device")
        val poiInfo = r.getAs[String]("poiInfo")
        val json = JSON.parseObject(poiInfo)
        res.::=(device, List(json))
      }
      res.toIterator
    }).reduceByKey((x, y) => {
      x ++ y
    })
    val schemaString = "device,feature,cnt,description"
    dineinRows.mapPartitions(maps => {
      val rdds = new ArrayBuffer[(String, String, String, String)]
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
          val value = wordFrequency(map1)
          val feature = s"${lbstype}_${tablefile}_${dayNum}"
          rdds += ((device, feature, value, ""))
        }
      }
      rdds.toIterator
    }).toDF(schemaString.split(","): _*).registerTempTable("offline_tmp")
    spark.sql(s"CACHE table offline_cache as select ${schemaString} from offline_tmp")
    spark.sql(s"select ${schemaString} from offline_cache")
      .coalesce(fileNum).registerTempTable("offline_coalsece")
    spark.sql(
      s"""insert overwrite table rp_sdk_dmp.timewindow_offline_profile
         |partition(flag=${lbstype},day=${parTime},timewindow=${dayNum})
         |select ${schemaString} from offline_coalsece
       """.stripMargin)
  }

  def getMapByKey(arr: List[JSONObject], key: String): Array[(String, String)] = {
    val c = ArrayBuffer[(String, String)]()
    if (key.contains(":")) {
      val getkey = key.split(":")(0)
      val splitStr = key.split(":")(1)
      for (map <- arr) {
        val key2 = map.get(getkey)
        val day = map.get("day").toString
        key2.toString.split(splitStr).foreach(f => {
          c += ((f, day))
        })
      }
    } else {
      for (map <- arr) {
        val key2 = map.get(key)
        val day = map.get("day").toString
        c += ((key2.toString, day))
      }
    }
    c.toArray
  }

  def wordFrequency(array: Array[(String, String)]): String = {
    val r = array.groupBy(_._1).map(f => {
      val name = f._1
      val cnt = f._2.distinct.length
      (name, cnt)
    })
    formatString(r)
  }

  def formatString(map: Map[String, Int]): String = {
    val keys = new StringBuilder
    val values = new StringBuilder
    var num = 0
    map.foreach { m =>
      if (num > 0) {
        keys.append(",").append(m._1)
        values.append(",").append(m._2)
      } else {
        keys.append(m._1)
        values.append(m._2)
        num += 1
      }
    }
    keys.append("=").append(values).toString()
  }
}