package com.mob.mid_full

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ConnectedAllUnid {


  def main(args: Array[String]): Unit = {

    // 构造图的边

    val spark = SparkSession.builder().appName("Step2TokenConnectedComponents")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val maxValue = args(0).toInt
    val inputTable = args(1)
    val outputTable = inputTable


    val df: DataFrame = spark.sql(
      s"""
         |select old_id,new_id from $inputTable
         |where version='all'
         |""".stripMargin
    )

    val edgeRdd: RDD[Edge[String]] = df.rdd
      .filter(raw => isIntByRegex(raw.getAs[String](0)) && isIntByRegex(raw.getAs[String](1)))
      .mapPartitions(
        iterator => {
          iterator.map(
            raw => Edge(raw.getAs[String](0).toLong, raw.getAs[String](1).toLong, "1")
          )
        })


    val df2 = spark.sql(
      s"""
         |select old_id from $inputTable where version='all'
         | group by old_id
         |""".stripMargin)
    // 顶点token
    val verticexRdd: RDD[(Long, String)] = df2.rdd.filter(raw => isIntByRegex(raw.getAs[String](0)))
      .mapPartitions(
        iterator => {
          iterator.map(raw => (raw.getAs[String](0).toLong, "a"))
        }
      )

    val graph = Graph(verticexRdd, edgeRdd)

    val ccGraph: Graph[VertexId, String] = graph.connectedComponents(maxValue)

    // ccGraph.vertices.saveAsTextFile("/tmp/ccgraph")

    ccGraph.vertices.map(
      x => (x._1.toString, x._2.toString)
    ).toDF("old_id", "new_id").createOrReplaceTempView("tmp_ccgraph_result")

    spark.sql(
      s"""
         |insert overwrite table $outputTable partition(month='2019-2021', version='all')
         |select old_id,new_id from tmp_ccgraph_result
         |""".stripMargin)

    spark.stop()
  }


  def isIntByRegex(s: String): Boolean = {
    val pattern = """^(\d+)$""".r
    s match {
      case pattern(_*) => true
      case _ => false
    }
  }
}
