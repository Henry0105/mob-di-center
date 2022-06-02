package com.mob.mid_repair

import org.apache.spark.sql.SparkSession

object Id2Vertex2graph {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)
    val id: String = args(1)

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(s"${id}_Vertex_$day")
      .getOrCreate()

    compute(spark, day, id)

    spark.stop()

  }

  def compute(spark: SparkSession, day: String, id: String): Unit = {

    //去黑名单
    val sql: String =
      s"""
         |SELECT ${if (id == "oiid") s"CONCAT($id,'_',factory) AS oiid" else id}
         |     , unid
         |FROM
         |(
         |  SELECT a.*
         |  FROM
         |  (
         |    SELECT *
         |    FROM dm_mid_master.dwd_all_id_detail
         |    WHERE day = '$day'
         |    AND $id <> ''
         |    AND $id IS NOT NULL
         |  )a
         |  LEFT ANTI JOIN dm_mid_master.ids_${id}_blacklist b
         |  ON a.${id} = b.${id}
         |)c
         |GROUP BY ${if (id == "oiid") s"CONCAT($id,'_',factory)" else id},unid
         |""".stripMargin
    println(sql)
    spark.sql(sql).createOrReplaceTempView(s"duid_${id}_unid")

    //构边
    spark.udf.register[Seq[(String, String, Int)], Seq[String]]("openid_resembled", openid_resembled)
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE mobdi_test.duid_vertex_di PARTITION(day = '$day', flag = '$id')
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
         |      FROM duid_${id}_unid
         |      GROUP BY $id
         |    )e
         |  )f
         |  LATERAL VIEW EXPLODE(tid_list) tmp AS tid_info
         |)g
         |GROUP BY id1,id2
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
