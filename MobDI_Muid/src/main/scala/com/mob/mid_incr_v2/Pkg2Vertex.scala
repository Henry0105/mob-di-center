package com.mob.mid_incr_v2

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object Pkg2Vertex {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)
    val pday: String = args(1)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"Pkg2Vertex_$day")
      .enableHiveSupport()
      .getOrCreate()

    compute(spark, day, pday)

    spark.stop()

  }

  def compute(spark: SparkSession, day: String, pday: String): Unit = {

    //1.与近一个月数据合并后进行图计算
    val duid_info_month: DataFrame = spark.sql(
      s"""
         |SELECT duid
         |     , pkg_it
         |     , unid
         |FROM dm_mid_master.duid_incr_tmp
         |WHERE day = '$day'
         |AND pkg_it <> ''
         |GROUP BY duid,pkg_it,unid
         |""".stripMargin)
    duid_info_month.cache()
    duid_info_month.count()
    duid_info_month.createOrReplaceTempView("duid_info_month")

    //2.过滤异常数据不参与图计算
    //2.1.找到安装量正常的pkg_it
    spark.sql(
      s"""
         |SELECT pkg_it
         |     , COUNT(1) AS cnt
         |FROM duid_info_month
         |GROUP BY pkg_it
         |HAVING cnt > 1
         |AND cnt < 5000
         |""".stripMargin).createOrReplaceTempView("normal_behavior_pkg_it")

    //2.2.找到各版本下安装量过多的duid
    spark.udf.register("get_pkg_ver", getPkgVersion _)
    spark.sql(
      s"""
         |SELECT unid
         |FROM
         |(
         |  SELECT unid
         |       , get_pkg_ver(pkg_it) AS version
         |       , count(1) AS cnt
         |  FROM duid_info_month
         |  GROUP BY unid,get_pkg_ver(pkg_it)
         |)a
         |WHERE cnt > 100
         |GROUP BY unid
         |""".stripMargin).createOrReplaceTempView("black_unid")

    spark.sql(
      """
        |SELECT c.duid
        |     , c.pkg_it
        |     , c.unid
        |FROM
        |(
        |  SELECT *
        |  FROM duid_info_month a
        |  LEFT ANTI JOIN black_unid b
        |  ON a.unid = b.unid
        |)c
        |LEFT SEMI JOIN
        |(
        |  SELECT pkg_it AS pi
        |  FROM normal_behavior_pkg_it
        |)d
        |ON c.pkg_it = d.pi
        |""".stripMargin).createOrReplaceTempView("incr_clear")

    spark.sql(
      s"""
         |SELECT duid
         |     , pkg_it
         |     , unid
         |FROM
         |(
         |  SELECT duid
         |       , pkg_it
         |       , unid
         |  FROM incr_clear
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , pkg_it
         |       , unid_final AS unid
         |  FROM dm_mid_master.duid_unid_info_month
         |  WHERE day = '$pday'
         |  AND SUBSTRING(pkg_it,-3) <> '000'
         |) a
         |GROUP BY duid,pkg_it,unid
         |""".stripMargin).createOrReplaceTempView("duid_info_all")

    //3.去除异常数据后构造边
    spark.udf.register[Seq[(String, String, Int)], Seq[String]]("openid_resembled", openid_resembled)
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_vertex_di PARTITION(day = '$day', flag = 'pkg')
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
         |      FROM duid_info_all
         |      GROUP BY pkg_it
         |    )e
         |  )f
         |  LATERAL VIEW EXPLODE(tid_list) tmp AS tid_info
         |)g
         |GROUP BY id1,id2
         |HAVING COUNT(1) >= 7
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

  private def getPkgVersion(pkg_it: String): String = {
    StringUtils.substringBeforeLast(pkg_it, "_")
  }
}
