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

    //更新ieid_oiid_asid关系数据
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

    //更新duid_ieid_oiid关系
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_ieid_oiid_base PARTITION(day = '$day')
         |SELECT duid
         |     , ieid
         |     , oiid
         |FROM
         |(
         |  SELECT duid
         |       , ieid
         |       , oiid
         |  FROM dm_mid_master.duid_incr_tmp
         |  WHERE (ieid <> '' OR oiid <> '')
         |  AND duid <> ''
         |  AND day = '$day'
         |  GROUP BY ieid,oiid,duid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , ieid
         |       , oiid
         |  FROM dm_mid_master.duid_ieid_oiid_base
         |  WHERE day = '$pday'
         |)a
         |GROUP BY duid,ieid,oiid
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
           |LEFT ANTI JOIN
           |(
           |  SELECT $id
           |  FROM dm_mid_master.${id}_blacklist_full
           |  WHERE day < '$day'
           |)b
           |ON a.$id = b.$id
           |""".stripMargin).createOrReplaceTempView(s"${id}_tmp")

      id match {
        case "ieid" => generateBlacklist(spark, "ieid,oiid,asid", 5, 10, "20211101")
        case "oiid" => generateBlacklist(spark, "oiid,ieid,asid", 3, 10, "20211101")
        case "asid" => generateBlacklist(spark, "asid,ieid,oiid", 3, 5, "20211101")
      }
    })

    //将dm_mid_master.duid_incr_tmp中再清洗下黑名单
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_incr_tmp PARTITION(day = '$day')
         |SELECT a.duid
         |     , a.factory
         |     , a.pkg_it
         |     , a.ieid
         |     , a.oiid
         |     , a.asid
         |     , a.unid
         |     , a.flag
         |     , a.source
         |     , a.duid_final
         |FROM
         |(
         |  SELECT *
         |  FROM dm_mid_master.duid_incr_tmp
         |  WHERE day = '$day'
         |)a
         |LEFT ANTI JOIN
         |(
         |  SELECT ieid
         |  FROM dm_mid_master.ieid_blacklist_full
         |  WHERE day <= '$day'
         |)b ON a.ieid = b.ieid
         |LEFT ANTI JOIN
         |(
         |  SELECT oiid
         |  FROM dm_mid_master.oiid_blacklist_full
         |  WHERE day <= '$day'
         |)c ON a.oiid = c.oiid
         |LEFT ANTI JOIN
         |(
         |  SELECT asid
         |  FROM dm_mid_master.asid_blacklist_full
         |  WHERE day <= '$day'
         |)d ON a.asid = d.asid
         |""".stripMargin)

    spark.stop()

  }

  def generateBlacklist(spark: SparkSession, ids: String, count1: Int, count2: Int, day: String): DataFrame = {

    val id: Array[String] = ids.split(",")

    spark.sql(
      s"""
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
         |""".stripMargin).createOrReplaceTempView(s"${id(0)}_blacklist_$day")

    if (id(0) == "asid") {
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE dm_mid_master.${id(0)}_blacklist_full PARTITION(day='$day')
           |SELECT ${id(0)}
           |FROM ${id(0)}_blacklist_$day
           |""".stripMargin)
    } else {
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE dm_mid_master.${id(0)}_blacklist_full PARTITION(day='$day')
           |SELECT ${id(0)}
           |FROM
           |(
           |  SELECT ${id(0)}
           |  FROM
           |  (
           |    SELECT ${id(0)}
           |         , duid
           |    FROM
           |    (
           |      SELECT /*+ BROADCAST(b) */
           |             duid
           |           , ${id(0)}
           |      FROM
           |      (
           |        SELECT duid
           |             , ${id(0)}
           |        FROM dm_mid_master.duid_ieid_oiid_base
           |        WHERE day = '$day'
           |      )a
           |      LEFT ANTI JOIN
           |      (
           |        SELECT ${id(0)}
           |        FROM dm_mid_master.${id(0)}_blacklist_full
           |        WHERE day < '$day'
           |      )b
           |      ON a.${id(0)} = b.${id(0)}
           |    )c
           |    GROUP BY ${id(0)},duid
           |  )d
           |  GROUP BY ${id(0)}
           |  HAVING COUNT(1) > 200
           |
           |  UNION ALL
           |
           |  SELECT ${id(0)}
           |  FROM ${id(0)}_blacklist_$day
           |)e
           |GROUP BY ${id(0)}
           |""".stripMargin)
    }

  }
}
