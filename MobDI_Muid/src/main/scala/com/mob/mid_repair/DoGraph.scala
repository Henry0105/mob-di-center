package com.mob.mid_repair

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.matching.Regex

object DoGraph {

  def main(args: Array[String]): Unit = {

    val vertexsql: String = args(0)
    val verticexsql: String = args(1)
    val connectedComponents: Int = args(2).toInt
    val output: String = args(3)

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(s"Graph")
      .getOrCreate()

    compute(spark, vertexsql, verticexsql, connectedComponents, output)

    spark.stop()

  }

  def compute(spark: SparkSession, vertexsql: String, verticexsql: String, connectedComponents: Int, output: String): Unit = {
    println(vertexsql)

    val vertex: DataFrame = spark.sql(s"$vertexsql")

    //构造边
    val edgeRdd: RDD[Edge[String]] = makeEdge(vertex)

    //构造顶点
    val verticex: DataFrame = spark.sql(s"$verticexsql")
    val verticexRdd: RDD[(VertexId, String)] = makeVerticex(verticex)

    //构造图
    val graph = Graph(verticexRdd, edgeRdd)
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents(connectedComponents)

    val value: RDD[(String, String)] = ccGraph.vertices.map(x => (x._1.toString, x._2.toString))

    spark.createDataFrame(value).toDF("unid", "unid_final").createOrReplaceTempView("tmp_ccgraph_result")

    spark.sql(s"create table $output select * from tmp_ccgraph_result")

    spark.stop()

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
