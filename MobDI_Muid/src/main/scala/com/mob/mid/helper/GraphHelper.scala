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
         |FROM ${defaultParam.vertexTable}
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
         |INSERT OVERWRITE TABLE ${defaultParam.outputTable} PARTITION(day = '${defaultParam.day}')
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
         |SELECT duid
         |     , pkg_it
         |     , version
         |     , unid
         |FROM
         |(
         |  SELECT c.duid
         |       , c.pkg_it
         |       , c.version
         |       , c.unid
         |       , c.pkg_it_cnt
         |       , COUNT(1) OVER(PARTITION BY duid) AS pkg_it_abnormal_cnt
         |  FROM
         |  (
         |    SELECT a.*,COUNT(1) OVER(PARTITION BY duid) AS pkg_it_cnt
         |    FROM
         |    (
         |      SELECT *
         |      FROM duid_info_unidfinal
         |      WHERE flag = 0
         |    ) a
         |    LEFT ANTI JOIN black_duid b
         |    ON a.duid = b.duid
         |  )c
         |  LEFT ANTI JOIN
         |  (
         |    SELECT pkg_it AS pi
         |    FROM normal_behavior_pkg_it
         |  )d
         |  ON c.pkg_it = d.pi
         |)e
         |WHERE pkg_it_abnormal_cnt = pkg_it_cnt
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

    //只需把图计算后的数据更新进dm_mid_master.old_new_unid_mapping_par
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${defaultParam.unidFinalTable} PARTITION(month = '${defaultParam.day}',version = 'all')
         |SELECT oid_id
         |     , new_id
         |FROM
         |(
         |  SELECT oid_id
         |       , new_id
         |  FROM ${defaultParam.unidFinalTable}
         |  WHERE month = '2019-2021'
         |  AND version = 'all'
         |
         |  UNION ALL
         |
         |  SELECT unid AS oid_id
         |       , unid_final AS new_id
         |  FROM tmp_ccgraph_result
         |)a
         |GROUP BY oid_id,new_id
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
