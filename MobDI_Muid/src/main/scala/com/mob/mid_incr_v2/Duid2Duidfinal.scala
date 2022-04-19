package com.mob.mid_incr_v2

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.matching.Regex

object Duid2Duidfinal {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(s"Duid2Duidfinal_$day")
      .getOrCreate()

    compute(spark, day)

    spark.stop()

  }

  def compute(spark: SparkSession, day: String): DataFrame = {

    val vertex: DataFrame = spark.sql(
      s"""
         |SELECT id1,id2
         |FROM dm_mid_master.duid_vertex_di
         |WHERE day = '$day'
         |AND id1 IS NOT NULL
         |AND id1 <> ''
         |AND id2 IS NOT NULL
         |AND id2 <> ''
         |GROUP BY id1,id2
         |""".stripMargin)

    //构造边
    val edgeRdd: RDD[Edge[String]] = makeEdge(vertex)

    //构造顶点
    val verticex: DataFrame = spark.sql(s"SELECT unid FROM dm_mid_master.duid_incr_tmp WHERE day = '$day' GROUP BY unid")
    val verticexRdd: RDD[(VertexId, String)] = makeVerticex(verticex)

    //构造图
    val graph = Graph(verticexRdd, edgeRdd)
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents(20)

    val value: RDD[(String, String)] = ccGraph.vertices.map(x => (x._1.toString, x._2.toString))

    spark
      .createDataFrame(value)
      .toDF("unid", "unid_final")
      .createOrReplaceTempView("tmp_ccgraph_result")

    //当日duid-unid-unidfinal数据
    val duid_unid_unidfinal_incr: DataFrame = spark.sql(
      s"""
         |SELECT a.duid
         |     , a.duid_final
         |     , a.unid
         |     , a.pkg_it
         |     , a.ieid
         |     , a.oiid
         |     , a.factory
         |     , a.flag
         |     , COALESCE(b.unid_final,a.unid) AS unid_final
         |FROM
         |(
         |  SELECT *
         |  FROM dm_mid_master.duid_incr_tmp
         |  WHERE day = '$day'
         |)a
         |LEFT JOIN tmp_ccgraph_result b
         |ON a.unid = b.unid
         |""".stripMargin)
    duid_unid_unidfinal_incr.cache()
    duid_unid_unidfinal_incr.count()
    duid_unid_unidfinal_incr.createOrReplaceTempView("duid_unid_unidfinal_incr")

    //生成每日duid-unidfinal数据
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_unid_info_di PARTITION(day = '$day')
         |SELECT duid
         |     , pkg_it
         |     , unid_final
         |FROM duid_unid_unidfinal_incr
         |WHERE pkg_it <> ''
         |GROUP BY duid,pkg_it,unid_final
         |""".stripMargin)

    //将当日新生成的duid_duidfinal的关系更新
    spark.sql(
      s"""
         |WITH unid_duidfinal AS (
         |  SELECT unid
         |       , duid_final
         |  FROM duid_unid_unidfinal_incr
         |  GROUP BY unid,duid_final
         |)
         |
         |INSERT OVERWRITE TABLE dm_mid_master.duid_duidfinal_info_incr PARTITION (day = '$day')
         |SELECT c.duid
         |       , COALESCE(c.duid_final,d.duid_final) AS duid_final
         |       , ieid
         |       , oiid
         |       , factory
         |       , flag
         |       , c.unid_final
         |       , c.unid
         |FROM
         |(
         |  SELECT a.duid
         |       , b.duid_final AS duid_final
         |       , ieid
         |       , oiid
         |       , factory
         |       , flag
         |       , unid_final
         |       , unid
         |  FROM
         |  (
         |      SELECT duid
         |           , unid_final
         |           , ieid
         |           , oiid
         |           , factory
         |           , flag
         |           , unid
         |      FROM duid_unid_unidfinal_incr
         |      GROUP BY duid,unid_final,ieid,oiid,factory,flag,unid
         |  ) a
         |  LEFT JOIN
         |  (
         |    SELECT duid_final
         |         , unid AS unid_tmp
         |    FROM unid_duidfinal
         |  )b
         |  ON a.unid_final = b.unid_tmp
         |)c
         |LEFT JOIN
         |(
         |  SELECT unid
         |       , duid_final
         |  FROM dm_mid_master.duid_unidfinal_duidfinal_mapping
         |  WHERE day < '$day'
         |)d
         |ON c.unid_final = d.unid
         |""".stripMargin)

    //将当日新生成的duid_unidfinal的关系更新
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_unidfinal_duidfinal_mapping PARTITION(day = '$day')
         |SELECT duid
         |     , unid
         |     , unid_final
         |     , duid_final
         |FROM dm_mid_master.duid_duidfinal_info_incr
         |WHERE day = '$day'
         |AND flag = 1
         |GROUP BY duid,unid,unid_final,duid_final
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

  private def makeVerticex(verticex: DataFrame): RDD[(VertexId, String)] = {
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
