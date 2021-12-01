package com.mob.mid.helper

import com.mob.mid.bean.Param
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Pkg2VertexHelper {

  def makeEdge(spark: SparkSession, defaultParam: Param): DataFrame = {

    //1.取近当天数据进行厂商机型清洗
    spark.sql(
      s"""
         |SELECT /*+ broadcast(d) */
         |       c.duid
         |     , c.pkg_it
         |     , IF(d.category IS NOT NULL,CONCAT(c.factory,'_',d.category),'other_10000') AS version
         |     , day
         |FROM
         |(
         |    SELECT /*+ broadcast(b) */
         |           a.duid
         |         , a.pkg_it
         |         , CASE
         |             WHEN LOWER(TRIM(a.factory)) in ('null','none','na','other')
         |                  OR a.factory IS NULL
         |                  OR TRIM(UPPER(a.factory)) IN ('','未知','UNKNOWN')
         |             THEN 'unknown'
         |           ELSE coalesce(UPPER(TRIM(b.clean_brand_origin)), 'other')
         |           END AS factory
         |         , a.model
         |         , day
         |    FROM
         |    (
         |        SELECT duid
         |             , factory
         |             , model
         |             , CONCAT(pkg,'_',version,'_',firstinstalltime) AS pkg_it
         |             , day
         |        FROM dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
         |        WHERE day = '${defaultParam.day}'
         |        AND firstinstalltime IS NOT NULL
         |        AND LENGTH(firstinstalltime) = 13
         |        AND LENGTH(pkg) > 0
         |        AND SUBSTR(firstinstalltime,-3,3) <> '000'
         |    )a
         |    LEFT JOIN dm_sdk_mapping.brand_model_mapping_par b
         |    ON b.version = '1000' AND UPPER(TRIM(b.brand)) = UPPER(TRIM(a.factory)) AND UPPER(TRIM(b.model)) = UPPER(TRIM(a.model))
         |)c
         |LEFT JOIN dm_mobdi_tmp.dim_muid_factory_model_category d
         |ON c.factory = UPPER(TRIM(d.factory)) AND c.model = UPPER(TRIM(d.model))
         |""".stripMargin).createOrReplaceTempView("duid_pkgit_version")

    //2.每日duid与duid_fsid_mapping匹配，找到对应雪花id，找不到则生成新的对应关系
    spark.sql("create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF'")
    val fsidDf: DataFrame = spark.sql(
      s"""
         |SELECT a.duid
         |     , a.pkg_it
         |     , a.version
         |     , IF(b.duid IS NULL,fsid(${defaultParam.day}),b.sfid) AS unid
         |     , IF(b.duid IS NULL,1,0) AS flag
         |     , day
         |FROM duid_pkgit_version a
         |LEFT JOIN
         |(
         |  SELECT duid
         |       , sfid
         |  FROM dm_mid_master.duid_unid_mapping
         |  WHERE version = '2019-2021'
         |)b ON a.duid = b.duid
         |""".stripMargin)
    fsidDf.cache()
    fsidDf.count()
    fsidDf.createOrReplaceTempView("duid_pkgit_version_unid_incr")

    //3.将今天新的duid-unid信息更新进入duid_fsid_mapping
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_unid_mapping PARTITION(version = '${defaultParam.day}')
         |SELECT duid
         |     , unid AS sfid
         |FROM duid_pkgit_version_unid_incr
         |WHERE flag = 1
         |""".stripMargin)

    //4.通过old_new_unid_mapping_par拿到图计算后的unid_final
    spark.sql(
      """
        |SELECT duid
        |     , pkg_it
        |     , version
        |     , IF(b.old_id IS NULL,unid,b.new_id) AS unid
        |     , IF(b.old_id IS NULL,0,1) AS flag
        |FROM duid_pkgit_version_unid_incr a
        |LEFT JOIN
        |(
        |  SELECT old_id
        |       , new_id
        |  FROM dm_mid_master.old_new_unid_mapping_par
        |  WHERE month = '2019-2021'
        |  AND version = 'all'
        |)b
        |ON a.unid = b.old_id
        |""".stripMargin).createOrReplaceTempView("duid_info_unidfinal")

    val duid_info_month: DataFrame = spark.sql(
      s"""
         |SELECT duid
         |     , pkg_it
         |     , version
         |     , unid
         |FROM duid_info_unidfinal
         |WHERE flag = 0
         |
         |union all
         |
         |SELECT duid,
         |     , pkg_it
         |     , version
         |     , unid
         |FROM dm_mid_master.duid_unid_info_month
         |WHERE day = '${defaultParam.day}'
         |""".stripMargin)
    duid_info_month.cache()
    duid_info_month.count()
    duid_info_month.createOrReplaceTempView("duid_info_month")

    //5.过滤异常数据不参与图计算
    //5.1.找到安装量正常的pkg_it
    spark.sql(
      s"""
         |SELECT pkg_it
         |     , COUNT(1) AS cnt
         |FROM duid_info_month
         |GROUP BY pkg_it
         |HAVING cnt > 1 AND cnt < ${defaultParam.pkgItLimit}
         |""".stripMargin).createOrReplaceTempView("normal_behavior_pkg_it")

    //5.2.找到各版本下安装量过多的duid
    spark.sql(
      s"""
         |SELECT duid
         |FROM
         |(
         |  SELECT duid
         |       , SPLIT(pkg_it,'_')[1] AS version
         |       , count(1) AS cnt
         |  FROM duid_info_month
         |  GROUP BY duid,SPLIT(pkg_it,'_')[1]
         |)a
         |WHERE cnt > ${defaultParam.pkgReinstallTimes}
         |GROUP BY duid
         |""".stripMargin).createOrReplaceTempView("black_duid")

    //black_duid落表
    spark.sql("""""")

    //6.去除异常数据后构造边
    spark.udf.register[Seq[(String, String, Int)], Seq[String]]("openid_resembled", openid_resembled)
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_vertex_di PARTITION(day = '${defaultParam.day}')
         |SELECT id1,id2
         |FROM
         |(
         |  SELECT tid_info._1 AS id1
         |       , tid_info._2 AS id2
         |  FROM
         |  (
         |    SELECT openid_resembled(tids) AS tid_list
         |    FROM
         |    (
         |      SELECT collect_set(unid) AS tids
         |      FROM
         |      (
         |        SELECT *
         |        FROM duid_info_month a
         |        LEFT ANTI JOIN black_duid b
         |        ON a.duid = b.duid
         |      )c
         |      INNER JOIN duid_info_month d
         |      ON c.pkg_it = d.pkg_it
         |      GROUP BY c.pkg_it
         |    )e
         |  )f
         |  LATERAL VIEW EXPLODE(tid_list) tmp AS tid_info
         |)g
         |GROUP BY id1,id2
         |HAVING COUNT(1) > ${defaultParam.edgeLimit}
         |""".stripMargin)

  }

  private def openid_resembled(ids: Seq[String]): Seq[(String, String, Int)] = {
    import scala.collection.mutable.ArrayBuffer
    val openidList = ids.toList
    val out = new ArrayBuffer[(String, String, Int)]()
    for (i <- openidList.indices) {
      val sourceOpenid = openidList(i)
      for (j <- i + 1 until openidList.size) {
        val targetOpenid = openidList(j)
        if (sourceOpenid.compareTo(targetOpenid) > 0) {
          out += ((sourceOpenid, targetOpenid, 1))
        } else {
          out += ((targetOpenid, sourceOpenid, 1))
        }
      }
    }
    out
  }

}
