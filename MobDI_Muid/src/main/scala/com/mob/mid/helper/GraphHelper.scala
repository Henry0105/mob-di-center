package com.mob.mid.helper

import com.mob.mid.bean.Param
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object GraphHelper {

  def compute(spark: SparkSession, defaultParam: Param): DataFrame = {

    val vertex: DataFrame = spark.sql(
      s"""
         |SELECT id1,id2
         |FROM dm_mid_master.duid_vertex_di
         |WHERE day = '${defaultParam.day}'
         |AND id1 IS NOT NULL
         |AND id1 <> ''
         |AND id2 IS NOT NULL
         |AND id2 <> ''
         |""".stripMargin)

    //构造边
    val edgeRdd: RDD[Edge[String]] = makeEdge(vertex)

    //构造顶点
    val verticex: Dataset[Row] = vertex.select("id1").distinct()
    val verticexRdd: RDD[(VertexId, String)] = makeVerticex(verticex)

    //构造图
    val graph = Graph(verticexRdd, edgeRdd)
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents(defaultParam.graphConnectTimes)

    val value: RDD[(String, String)] = ccGraph
      .vertices
      .map(x => (x._1.toString, x._2.toString))

    spark
      .createDataFrame(value)
      .toDF("unid", "unid_final")
      .createOrReplaceTempView("tmp_ccgraph_result")

    //生成每日duid-unidfinal数据
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_unid_info_di PARTITION(day = '${defaultParam.day}')
         |SELECT duid
         |     , pkg_it
         |     , version
         |     , unid
         |FROM duid_info_unidfinal
         |WHERE flag = 1
         |
         |UNION ALL
         |
         |SELECT a.duid
         |     , a.pkg_it
         |     , a.version
         |     , a.unid
         |FROM
         |(
         |  SELECT *
         |  FROM duid_info_unidfinal
         |  WHERE flag = 0
         |)a
         |LEFT SEMI JOIN black_duid b
         |ON a.duid = b.duid
         |
         |UNION ALL
         |
         |SELECT c.duid
         |     , c.pkg_it
         |     , c.version
         |     , c.unid
         |FROM
         |(
         |  SELECT a.*
         |  FROM
         |  (
         |    SELECT *
         |    FROM duid_info_unidfinal
         |    WHERE flag = 0
         |  ) a
         |  LEFT ANTI JOIN black_duid b
         |  ON a.duid = b.duid
         |)c
         |LEFT ANTI JOIN duid_info_month d
         |ON c.pkg_it = d.pkg_it
         |
         |UNION ALL
         |
         |SELECT a.duid
         |     , a.pkg_it
         |     , a.version
         |     , b.unid_final AS unid
         |FROM
         |(
         |  SELECT *
         |  FROM duid_info_unidfinal
         |  WHERE flag = 0
         |)a
         |INNER JOIN tmp_ccgraph_result b
         |ON a.unid = b.unid
         |""".stripMargin)

    //更新dm_mid_master.old_new_duid_mapping_par_tmp
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.old_new_duid_mapping_par_tmp PARTITION(day = )
         |SELECT COALESCE(a.oid_id,b.unid) AS oid_id
         |     , COALESCE(a.new_id,b.unid_final) AS new_id
         |FROM
         |(
         |  SELECT oid_id
         |       , new_id
         |  FROM dm_mid_master.old_new_duid_mapping_par_tmp
         |  WHERE month =
         |)a
         |FULL JOIN
         |(
         |  SELECT a.unid
         |       , IF(b.unid IS NULL,a.unid,b.unid_final) AS unid_final
         |  FROM duid_pkgit_version_unid_incr x
         |  LEFT JOIN tmp_ccgraph_result y
         |  ON x.unid = y.unid
         |)b
         |ON a.oid_new = b.unid
         |
         |换成 UNION ALL 再 GROUP BY
         |""".stripMargin)


  }

  private def makeEdge(vertex: Dataset[Row]): RDD[Edge[String]] = {
    vertex
      .rdd
      .filter(raw => isIntByRegex(raw.getAs[String](0)) && isIntByRegex(raw.getAs[String](1)))
      .mapPartitions(
        iterator => {
          iterator.map(raw => Edge(raw.getAs[String](0).toLong, raw.getAs[String](1).toLong, "1"))
        }
      )
  }

  private def makeVerticex(verticex: Dataset[Row]): RDD[(VertexId, String)] = {
    verticex
      .rdd
      .filter(raw => isIntByRegex(raw.getAs[String](0)))
      .mapPartitions(
        iterator => {
          iterator.map(raw => (raw.getAs[String](0).toLong, "a"))
        }
      )
  }

  private def isIntByRegex(s: String): Boolean = {
    val pattern: Regex = """^(\d+)$""".r
    s match {
      case pattern(_*) => true
      case _ => false
    }
  }

}
