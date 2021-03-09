package com.youzu.mob.label

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author xdzhang
 *         description 餐饮行业标签
 *         根据device汇总每周或每月外卖，堂吃信息
 *         输出表dm_sdk_master.catering_lbs_label_weekly,dm_sdk_master.catering_lbs_label_monthly
 *         createTime 2017-11-10
 */
object CateringLbsDaily {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().
      setAppName("Spark Application " + this.getClass.getSimpleName.stripSuffix("$"))
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val day = args(0)
    val repartition = args(3).toInt

    val dinein_tmp_sql = args(1)
    val dineinRdd = hiveContext.sql(dinein_tmp_sql).repartition(repartition)
    dineinRdd.registerTempTable("device_catering_tmp")
    val dineinRows = dineinRdd.map(row => {
      val device = row.getAs[String]("device")
      val brand = row.getAs[String]("brand")
      val taste = row.getAs[String]("taste")
      val type1 = row.getAs[String]("type1")
      val type2 = row.getAs[String]("type2")
      val time = row.getAs[String]("time")
      val day = row.getAs[String]("day")
      (device, (brand, taste, type1, type2, time, day))
    }).groupByKey()

    var names = new StringBuilder
    var cnt = new StringBuilder
    val re = dineinRows.map(maprows => {
      val device = maprows._1
      val r = maprows._2.toArray

      val r_1 = r.groupBy(_._1).map { f =>
        val name = f._1
        val cnt = f._2.map(arr => (arr._1, arr._6)).distinct.length
        (name, cnt)
      }
      val r_2 = r.groupBy(_._2).map { f =>
        val name = f._1
        val cnt = f._2.map(arr => (arr._2, arr._6)).distinct.length
        (name, cnt)
      }
      val r_3 = r.groupBy(_._3).map { f =>
        val name = f._1
        val cnt = f._2.map(arr => (arr._3, arr._6)).distinct.length
        (name, cnt)
      }
      val r_4 = r.groupBy(_._4).map { f =>
        val name = f._1
        val cnt = f._2.map(arr => (arr._4, arr._6)).distinct.length
        (name, cnt)
      }
      val r_5 = r.groupBy(_._5).map { f =>
        val name = f._1
        val cnt = f._2.map(arr => (arr._5, arr._6)).distinct.length
        (name, cnt)
      }

      (device, formatString(r_1), formatString(r_2), formatString(r_3), formatString(r_4), formatString(r_5))
    })
    val schemaString = "device,catering_dinein_brand_detail,catering_dinein_taste_detail," +
      "catering_dinein_tyle1_detail,catering_dinein_tyle2_detail,catering_dinein_time_detail"
    val dschema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val reRow = re.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6))
    hiveContext.createDataFrame(reRow, dschema).registerTempTable("dinein_tmp")

    val dinein_sql = args(2)
    hiveContext.sql(dinein_sql)
    sc.stop()
  }

  def formatString(map: Map[String, Int]): String = {
    val keys = new StringBuilder
    val values = new StringBuilder
    map.foreach { m =>
      if (keys.length > 0) {
        keys.append(",").append(m._1)
        values.append(",").append(m._2)
      } else {
        keys.append(m._1)
        values.append(m._2)
      }
    }
    keys.append("=").append(values).toString()
  }
}