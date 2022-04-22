package com.mob.mid_incr_v2

import org.apache.spark.sql.{DataFrame, SparkSession}

object IdBlackList {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)
    val id: String = args(1)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"BlackList_$day")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql(
      s"""
         |SELECT a.ieid
         |     , a.oiid
         |     , a.duid
         |FROM
         |(
         |  SELECT ieid
         |       , oiid
         |       , duid
         |  FROM dm_mid_master.duid_oiid_ieid_mapping_full
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
      case "ieid" => generateBlacklist(spark, "ieid,oiid,duid", 5, 200, day)
      case "oiid" => generateBlacklist(spark, "oiid,ieid,duid", 3, 200, day)
      case "duid" => generateBlacklist(spark, "duid,ieid,oiid", 3, 2, day)
    }

    spark.stop()

  }

  def generateBlacklist(spark: SparkSession, ids: String, count1: Int, count2: Int, day: String): Unit = {

    val id: Array[String] = ids.split(",")

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
         |         , ${id(1)}
         |    FROM ${id(0)}_tmp
         |    WHERE ${id(1)} <> ''
         |    GROUP BY ${id(0)},${id(1)}
         |  )a
         |  GROUP BY ${id(0)}
         |  HAVING COUNT(1) > $count1
         |
         |  UNION ALL
         |
         |  SELECT ${id(0)}
         |  FROM
         |  (
         |    SELECT ${id(0)}
         |         , ${id(2)}
         |    FROM ${id(0)}_tmp
         |    WHERE ${id(2)} <> ''
         |    GROUP BY ${id(0)},${id(2)}
         |  )b
         |  GROUP BY ${id(0)}
         |  HAVING COUNT(1) > $count2
         |)c
         |GROUP BY ${id(0)}
         |""".stripMargin)
  }
}
