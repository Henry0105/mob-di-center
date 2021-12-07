package com.mob.mid.helper

import com.mob.mid.bean.Param
import org.apache.spark.sql.{DataFrame, SparkSession}

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
         |        FROM ${defaultParam.inputTable}
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
    spark.sql(
      s"""
         |SELECT a.duid
         |     , a.pkg_it
         |     , a.version
         |     , b.sfid AS unid
         |     , IF(b.duid IS NULL,1,0) AS flag
         |     , day
         |FROM duid_pkgit_version a
         |LEFT JOIN
         |(
         |  SELECT duid
         |       , sfid
         |  FROM ${defaultParam.duidUnidTable}
         |)b ON a.duid = b.duid
         |""".stripMargin).createOrReplaceTempView("duid_pkgit_version_unid_incr_tmp")

    spark.sql("create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF'")
    spark.sql(
      s"""
         |SELECT duid
         |     , fsid('${defaultParam.day}') AS unid
         |FROM
         |(
         |  SELECT duid
         |  FROM duid_pkgit_version_unid_incr_tmp
         |  WHERE flag = 1
         |  GROUP BY duid
         |)a
         |""".stripMargin).createOrReplaceTempView("duid_fsid_incr")

    spark.sql(
      """
        |SELECT a.duid
        |     , a.pkg_it
        |     , a.version
        |     , IF(a.flag = 1,b.unid,a.unid) AS unid
        |FROM duid_pkgit_version_unid_incr_tmp a
        |LEFT JOIN duid_fsid_incr b
        |ON a.duid = b.duid
        |""".stripMargin).createOrReplaceTempView("duid_pkgit_version_unid_incr")

    //3.将今天新的duid-unid信息更新进入duid_fsid_mapping
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${defaultParam.duidUnidTable} PARTITION(version = '${defaultParam.day}')
         |SELECT duid
         |     , unid
         |FROM duid_fsid_incr
         |""".stripMargin)

    //4.通过old_new_unid_mapping_par拿到图计算后的unid_final
    val duid_info_unidfinal: DataFrame = spark.sql(
      s"""
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
         |  FROM ${defaultParam.unidFinalTable}
         |  WHERE month = '${defaultParam.pday}'
         |  AND version = 'all'
         |)b
         |ON a.unid = b.old_id
         |""".stripMargin)

    duid_info_unidfinal.cache()
    duid_info_unidfinal.count()
    duid_info_unidfinal.createOrReplaceTempView("duid_info_unidfinal")

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
         |SELECT duid
         |     , pkg_it
         |     , version
         |     , unid
         |FROM ${defaultParam.unidMonthTable}
         |WHERE day = '${defaultParam.pday}'
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
         |HAVING cnt > 1
         |AND cnt < ${defaultParam.pkgItLimit}
         |""".stripMargin).createOrReplaceTempView("normal_behavior_pkg_it")

    //5.2.找到各版本下安装量过多的duid
    spark.sql(
      s"""
         |SELECT duid
         |FROM
         |(
         |  SELECT duid
         |       , CONCAT(SPLIT(pkg_it,'_')[0],'_',SPLIT(pkg_it,'_')[1]) AS version
         |       , count(1) AS cnt
         |  FROM duid_info_month
         |  GROUP BY duid,CONCAT(SPLIT(pkg_it,'_')[0],'_',SPLIT(pkg_it,'_')[1])
         |)a
         |WHERE cnt > ${defaultParam.pkgReinstallTimes}
         |GROUP BY duid
         |""".stripMargin).createOrReplaceTempView("black_duid")

    //6.去除异常数据后构造边
    spark.udf.register[Seq[(String, String, Int)], Seq[String]]("openid_resembled", openid_resembled)
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${defaultParam.vertexTable} PARTITION(day = '${defaultParam.day}')
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
         |      SELECT collect_set(c.unid) AS tids
         |      FROM
         |      (
         |        SELECT *
         |        FROM duid_info_month a
         |        LEFT ANTI JOIN black_duid b
         |        ON a.duid = b.duid
         |      )c
         |      LEFT SEMI JOIN
         |      (
         |        SELECT pkg_it AS pi
         |        FROM normal_behavior_pkg_it
         |      )d
         |      ON c.pkg_it = d.pi
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
