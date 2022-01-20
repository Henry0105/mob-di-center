package com.mob.mid_incr

import org.apache.spark.sql.{DataFrame, SparkSession}

object IdBlackList {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)
    val pday: String = args(1)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"BlackList_$day")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.ieid_oiid_asid_base PARTITION(day = '$day')
         |SELECT ieid
         |     , oiid
         |     , asid
         |FROM
         |(
         |  SELECT ieid
         |       , oiid
         |       , asid
         |  FROM dm_mid_master.duid_incr_tmp
         |  WHERE (ieid <> '' OR oiid <> '' OR asid <> '')
         |  AND day = '$day'
         |  GROUP BY ieid,oiid,asid
         |
         |  UNION ALL
         |
         |  SELECT ieid
         |       , oiid
         |       , asid
         |  FROM dm_mid_master.ieid_oiid_asid_base
         |  WHERE day = '$pday'
         |)a
         |GROUP BY ieid,oiid,asid
         |""".stripMargin)

    List("ieid", "oiid", "asid").par.foreach(id => {
      spark.sql(
        s"""
           |SELECT /*+ BROADCAST(b) */
           |       a.ieid
           |     , a.oiid
           |     , a.asid
           |FROM
           |(
           |  SELECT ieid
           |       , oiid
           |       , asid
           |  FROM dm_mid_master.ieid_oiid_asid_base
           |  WHERE TRIM($id) <> ''
           |  AND day = '$day'
           |) a
           |LEFT ANTI JOIN dm_mid_master.${id}_blacklist_full b
           |ON a.${id} = b.${id}
           |""".stripMargin).createOrReplaceTempView(s"${id}_tmp")

      id match {
        case "ieid" => generateBlacklist(spark, "ieid,oiid,asid", 5, 10, day)
        case "oiid" => generateBlacklist(spark, "oiid,ieid,asid", 3, 10, day)
        case "asid" => generateBlacklist(spark, "asid,ieid,oiid", 3, 5, day)
      }
    })

    spark.stop()

  }

  def generateBlacklist(spark: SparkSession, ids: String, count1: Int, count2: Int, day: String): DataFrame = {

    val id: Array[String] = ids.split(",")

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.${id(0)}_blacklist_full PARTITION(day='$day')
         |SELECT ${id(0)}
         |FROM
         |(
         |  SELECT ${id(0)}
         |       , COLLECT_SET(${id(1)}) AS ${id(1)}_list
         |       , COLLECT_SET(${id(2)}) AS ${id(2)}_list
         |  FROM ${id(0)}_tmp
         |  GROUP BY ${id(0)}
         |)a
         |WHERE (SIZE(${id(1)}_list) > $count1 OR SIZE(${id(2)}_list) > $count2)
         |""".stripMargin)
  }
}
