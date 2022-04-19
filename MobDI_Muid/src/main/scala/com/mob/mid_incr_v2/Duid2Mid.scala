package com.mob.mid_incr_v2

import com.mob.deviceid.config.HiveProps
import com.mob.deviceid.udf._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

object Duid2Mid {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("duid2mid")
      .getOrCreate()

    /**
     * 数据分为两部分
     * 有ieid、oiid、asid的数据更新dm_mid_master.duid_mid_with_id_explode_final_fixed
     * 无id信息的更新dm_mid_master.duid_mid_without_id
     */

    spark.sql(
      s"""
         |SELECT duid
         |     , duid_final
         |     , ieid
         |     , oiid
         |     , asid
         |     , factory
         |     , IF(ieid = '' AND oiid = '' AND asid = '',1,0) AS id_flag
         |FROM dm_mid_master.duid_duidfinal_info_incr
         |WHERE day = '$day'
         |""".stripMargin).createOrReplaceTempView("duid_duidfinal_info_incr")

    //有id信息按照oiid-ieid-asid这个优先级匹配存量的MID
    //通过oiid匹配MID
    spark.sql(s"DROP TABLE IF EXISTS dm_mid_master.oiid_mid_$day")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dm_mid_master.oiid_mid_$day STORED AS ORC AS
         |SELECT duid
         |     , mid
         |FROM
         |(
         |  SELECT duid
         |       , mid
         |       , ROW_NUMBER(PARTITION BY oiid ORDER BY serdatetime) AS rk
         |  FROM
         |  (
         |    SELECT duid
         |         , mid
         |         , serdatetime
         |         , oiid
         |    FROM
         |    (
         |      SELECT duid
         |           , oiid
         |      FROM duid_duidfinal_info_incr
         |      WHERE oiid <> ''
         |      GROUP BY duid,oiid
         |    )a
         |    LEFT JOIN
         |    (
         |      SELECT oiid
         |           , mid
         |           , serdatetime
         |      FROM dm_mid_master.duid_mid_with_id_explode_final_fixed
         |      WHERE oiid <> ''
         |      GROUP BY oiid,mid,serdatetime
         |    ) b
         |    ON a.oiid = b.oiid
         |  )c
         |)e
         |WHERE rk = 1
         |""".stripMargin)

    //去除通过oiid能匹配到mid的数据，剩下数据再按照ieid匹配mid
    spark.sql(s"DROP TABLE IF EXISTS dm_mid_master.ieid_mid_$day")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dm_mid_master.ieid_mid_$day STORED AS ORC AS
         |SELECT duid
         |     , mid
         |FROM
         |(
         |  SELECT duid
         |       , mid
         |       , ROW_NUMBER(PARTITION BY ieid ORDER BY serdatetime) AS rk
         |  FROM
         |  (
         |    SELECT duid
         |         , mid
         |         , serdatetime
         |         , ieid
         |    FROM
         |    (
         |      SELECT duid
         |           , ieid
         |      FROM duid_duidfinal_info_incr a
         |      LEFT ANTI JOIN dm_mid_master.oiid_mid_$day b
         |      ON a.duid = b.duid
         |      WHERE a.ieid <> ''
         |      GROUP BY a.duid,a.ieid
         |    )a
         |    LEFT JOIN
         |    (
         |      SELECT ieid
         |           , mid
         |           , serdatetime
         |      FROM dm_mid_master.duid_mid_with_id_explode_final_fixed
         |      WHERE ieid <> ''
         |      GROUP BY ieid,mid,serdatetime
         |    ) b
         |    ON a.ieid = b.ieid
         |  )c
         |)e
         |WHERE rk = 1
         |""".stripMargin)

    //再去除通过ieid能匹配到mid的数据，剩下数据再按照ieid匹配mid
    spark.sql(s"DROP TABLE IF EXISTS dm_mid_master.asid_mid_$day")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dm_mid_master.asid_mid_$day STORED AS ORC AS
         |SELECT duid
         |     , mid
         |FROM
         |(
         |  SELECT duid
         |       , mid
         |       , ROW_NUMBER(PARTITION BY asid ORDER BY serdatetime) AS rk
         |  FROM
         |  (
         |    SELECT duid
         |         , mid
         |         , serdatetime
         |         , asid
         |    FROM
         |    (
         |      SELECT duid
         |           , asid
         |      FROM duid_duidfinal_info_incr a
         |      LEFT ANTI JOIN dm_mid_master.oiid_mid_$day b ON a.duid = b.duid
         |      LEFT ANTI JOIN dm_mid_master.ieid_mid_$day b ON a.duid = b.duid
         |      WHERE a.asid <> ''
         |      GROUP BY a.duid,a.asid
         |    )a
         |    LEFT JOIN
         |    (
         |      SELECT asid
         |           , mid
         |           , serdatetime
         |      FROM dm_mid_master.duid_mid_with_id_explode_final_fixed
         |      WHERE asid <> ''
         |      GROUP BY asid,mid,serdatetime
         |    ) b
         |    ON a.asid = b.asid
         |  )c
         |)e
         |WHERE rk = 1
         |""".stripMargin)

    spark.sql("create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';")
    spark.sql(
      s"""
         |SELECT a.duid
         |     , a.duid_final
         |     , a.ieid
         |     , a.oiid
         |     , a.asid
         |     , a.factory
         |     , COALESCE(b.mid,c.mid,d.mid,sha1(a.duid_final)) AS mid
         |     , unix_timestamp('$day','yyyyMMdd') AS serdatetime
         |FROM
         |(
         |  SELECT *
         |  FROM duid_duidfinal_info_incr
         |  WHERE id_flag = 0
         |)a
         |LEFT JOIN dm_mid_master.oiid_mid_$day b ON a.duid = b.duid
         |LEFT JOIN dm_mid_master.ieid_mid_$day c ON a.duid = c.duid
         |LEFT JOIN dm_mid_master.asid_mid_$day d ON a.duid = d.duid
         |""".stripMargin).createOrReplaceTempView("duid_id_mid")

    //更新dm_mid_master.duid_mid_with_id_explode_final_fixed
    spark.sql(
      """
        |INSERT OVERWRITE TABLE dm_mid_master.duid_mid_with_id_explode_final_fixed
        |SELECT duid
        |     , oiid
        |     , ieid
        |     , duid_final
        |     , asid
        |     , mid
        |     , factory
        |     , model
        |     , serdatetime
        |FROM
        |(
        |  SELECT duid
        |       , oiid
        |       , ieid
        |       , duid_final
        |       , asid
        |       , mid
        |       , factory
        |       , model
        |       , serdatetime
        |  FROM dm_mid_master.duid_mid_with_id_explode_final_fixed
        |
        |  UNION ALL
        |
        |  SELECT duid
        |       , oiid
        |       , ieid
        |       , duid_final
        |       , asid
        |       , mid
        |       , factory
        |       , '' as model
        |       , serdatetime
        |  FROM duid_id_mid
        |)a
        |GROUP BY duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime
        |""".stripMargin)


    //无id信息的更新dm_mid_master.duid_mid_without_id
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_mid_without_id
         |SELECT COALESCE(b.duid,a.duid) AS duid
         |     , COALESCE(b.duid_final,a.duid_final) AS duid_final
         |     , COALESCE(b.mid,sha1(a.duid_final)) AS mid
         |FROM
         |(
         |  SELECT duid
         |       , duid_final
         |  FROM duid_duidfinal_info_incr
         |  WHERE id_flag = 1
         |  GROUP BY duid,duid_final
         |)a
         |FULL JOIN dm_mid_master.duid_mid_without_id b
         |ON a.duid = b.duid
         |""".stripMargin)

  }
}


