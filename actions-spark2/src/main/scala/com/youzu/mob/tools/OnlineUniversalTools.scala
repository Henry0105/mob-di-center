package com.youzu.mob.tools


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession

import com.youzu.mob.utils.Constants._

object OnlineUniversalTools {
  def main(args: Array[String]): Unit = {
    val json = new createJson(args)
    val spark = SparkSession.builder().enableHiveSupport()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_${json.mappingtype}_${json.day}").getOrCreate()
    if (json.processfield.equals("")) {
      labelMatch(json, spark)
    } else {
      noLabelMatch(json, spark)
    }

  }

  def noLabelMatch(json: createJson, spark: SparkSession): Unit = {
  spark.sql(
    s"""
      |select ${json.processfield} ,count(1) as cnt from (
      |select ${json.processfield} ,day from
      |(${json.labelTable}) mm
      |where  mm.day >${json.bday} and mm.day <= ${json.day}
      |group by ${json.processfield},day )ff
      |group by ${json.processfield}
    """.stripMargin).registerTempTable("nolabel_tmp_data")
    spark.sql("select * from nolabel_tmp_data").registerTempTable("nolabel_online_tmp")
    spark.sql(
      s"select ${json.processfield},'0_${json.mappingtype}_${json.windowTime}' as feature,cnt from nolabel_online_tmp")
      .coalesce(json.fileNum).registerTempTable("nolabel_online_coalsece")
    spark.sql(
      s"""insert overwrite table
         |$TIMEWINDOW_ONLINE_PROFILE
         |partition(flag=${json.mappingtype},day=${json.day},timewindow=${json.windowTime})
         |select ${json.processfield},feature,cnt from nolabel_online_coalsece
       """.stripMargin)
  }

  class createJson(args: Array[String]) {
    var day = ""
    var mappingtype = ""
    var computeType = ""
    var fileNum = 0
    var repar = 0
    var windowTime = 0
    var labelTable = ""
    val json = args(0)
    var processfield = ""
    try {
      val ob = JSON.parseObject(json)
      day = ob.get("partition").toString
      fileNum = ob.get("fileNum").toString.toInt
      repar = ob.get("repartition").toString.toInt
      mappingtype = JSON.parseObject(ob.get("labelInfo").toString).get("mappingtype").toString
      computeType = JSON.parseObject(ob.get("labelInfo").toString).get("computeType").toString
      windowTime = JSON.parseObject(ob.get("labelInfo").toString).get("windowTime").toString.toInt
      labelTable = JSON.parseObject(ob.get("labelInfo").toString).get("labelTable").toString
      processfield = JSON.parseObject(ob.get("labelInfo").toString).get("processfield").toString
    } catch {
      case ex: Exception => throw new Exception(s"Json is not standardized!  Json=>${json}")
    }
    val bday = getBeforeTime(day, windowTime)
  }

  def labelMatch(json: createJson, spark: SparkSession): Unit = {
    var day = json.day
    var mappingtype = json.mappingtype
    var computeType = json.computeType
    var fileNum = json.fileNum
    var repar = json.repar
    var windowTime = json.windowTime
    var labelTable = json.labelTable
    val bday = json.bday

    import spark.implicits._
    spark.sql(
      s"""
         |cache table mappingtable as
         |select relation,category,total,percent from $ONLINE_CATEGORY_MAPPING
         |where type =${mappingtype}
       """.stripMargin)
    if ("".equals(labelTable)) {
//      spark.sql(
//        s"""
//           |select device , relation ,day  from
//           |dm_sdk_master.timewindow_online_data
//           |where type = '${mappingtype}'
//           |and mm.day >${bday} and mm.day <= ${day}
//      """.stripMargin).registerTempTable("sourcetable")
    } else {
      spark.sql(
        s"""
           |select device , relation ,day  from (
           |${labelTable}
           |) mm
           |where mm.day >${bday} and mm.day <= ${day}
      """.stripMargin).registerTempTable("sourcetable")
    }

    val data = spark.sql(
      """
        |select m.category,s.device,s.day,s.relation,m.percent,m.total from mappingtable m join
        |sourcetable s on s.relation = m.relation
      """.stripMargin)

    val rdd = data.repartition(repar).rdd.mapPartitions(rows => {
      var list = List[(String, Iterable[(String, String, String, Int, Int)])]()
      while (rows.hasNext) {
        val row = rows.next()
        val device = row.getAs[String]("device")
        val relation = row.getAs[String]("relation")
        val category = row.getAs[String]("category")
        val day = row.getAs[String]("day")
        val percent = row.getAs[Int]("percent")
        val total = row.getAs[Int]("total")
        list.::=(device, Iterable((relation, category, day, total, percent)))
      }
      list.toIterator
    }).reduceByKey((a, b) => {
      a ++ b
    })
    val schemaString = "device,feature,cnt"
    rdd.mapPartitions(rdds => {
      var list = List[(String, String, String)]()
      while (rdds.hasNext) {
        val rd = rdds.next()
        val device = rd._1
        val info = rd._2
        if ("1".equals(computeType)) {
          info.map(x => (x._2, x._3)).toArray.distinct.groupBy(_._1).map(m => {
            list.::=(device, m._1, m._2.length.toString)
          })
          val cnt = info.flatMap(f => {
            var relist = List[(String, String)]()
            if (f._4 == 1) {
              relist.::=("total", f._3)
            }
            relist
          }).toArray.distinct.length
          if (cnt > 0) {
            list.::=(device, "total", cnt.toString)
          }
        } else {
          info.map(x => (x._2, x._1)).toArray.distinct.groupBy(_._1).map(m => {
            list.::=(device, m._1, m._2.length.toString)
          })
          info.flatMap(f => {
            var relist = List[(String, String)]()
            if (f._4 == 1) {
              relist.::=("total", f._1)
            }
            if (f._5 == 1) {
              relist.::=("percent", f._1)
            }
            relist
          }).toArray.distinct.groupBy(_._1).map(m => {
            list.::=(device, m._1, m._2.length.toString)
          })
        }
      }
      list.toIterator
    }).toDF(schemaString.split(","): _*).createOrReplaceTempView("online_tmp")

    if ("1".equals(computeType)) {
      spark.sql(
        s"""CACHE table online_cache as
           |select ${schemaString} from online_tmp
         """.stripMargin)
    } else {
      spark.sql(
        """
          |select device,count(*) as count from (
          |select device,relation,Row_number() over(partition by device,relation order by device) as rank
          |from sourcetable group by device,relation) mm where mm.rank = 1 group by device
        """.stripMargin).createOrReplaceTempView("allcount")

      spark.sql(
        s"""CACHE table online_cache as
           |select online.device,feature,
           |case when feature ='percent' then online.cnt/c.count else online.cnt end as cnt
           |from online_tmp online
           |left join allcount c on c.device = online.device""".stripMargin)
    }

    spark.sql(s"select ${schemaString} from online_cache").coalesce(fileNum).createOrReplaceTempView("online_coalsece")
    spark.sql(
      s"""insert overwrite table $TIMEWINDOW_ONLINE_PROFILE
         |partition(flag=${mappingtype},day=${day},timewindow=${windowTime})
         |select ${schemaString} from online_coalsece
       """.stripMargin)
  }

  def getBeforeTime(time: String, bum: Int): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val a = sdf.parse(time)
    val cal = Calendar.getInstance()
    cal.setTime(a)
    cal.add(Calendar.DATE, -bum)
    sdf.format(cal.getTime())
  }

}
