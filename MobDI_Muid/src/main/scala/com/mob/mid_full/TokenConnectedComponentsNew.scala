package com.mob.mid_full

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object TokenConnectedComponentsNew {


  def main(args: Array[String]): Unit = {

    // 构造图的边

    val spark = SparkSession.builder().appName("Step2TokenConnectedComponents")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val maxValue = args(0).toInt
    val source = args(1)
    val outputTable = args(2)


    val df: DataFrame = spark.sql(source)

    val edgeRdd: RDD[Edge[String]] = df.rdd
      .filter(raw => isIntByRegex(raw.getAs[String](0)) && isIntByRegex(raw.getAs[String](1)))
      .mapPartitions(
        iterator => {
          iterator.map(
            raw => Edge(raw.getAs[String](0).toLong, raw.getAs[String](1).toLong, "1")
          )
        })


    val df2 = df.select("id1").distinct()
    // 顶点token
    val verticexRdd: RDD[(Long, String)] = df2.rdd.filter(raw => isIntByRegex(raw.getAs[String](0)))
      .mapPartitions(
        iterator => {
          iterator.map(raw => (raw.getAs[String](0).toLong, "a"))
        }
      )

    val m = StorageLevel.MEMORY_AND_DISK_2
    val graph = Graph(verticexRdd, edgeRdd, null.asInstanceOf[String], edgeStorageLevel = m, vertexStorageLevel = m)

    val ccGraph: Graph[VertexId, String] = graph.connectedComponents(maxValue)

    ccGraph.vertices.map(
      x => (x._1.toString, x._2.toString)
    ).toDF("old_id", "new_id").createOrReplaceTempView("tmp_ccgraph_result")

    if (args.length < 4) {
      spark.sql(
        s"""
           |insert overwrite table $outputTable
           |select old_id,new_id from tmp_ccgraph_result
           |""".stripMargin)
    } else {
      val partition = args(3)
      spark.sql(
        s"""
           |insert overwrite table $outputTable partition ($partition)
           |select old_id,new_id from tmp_ccgraph_result
           |""".stripMargin)
    }

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
