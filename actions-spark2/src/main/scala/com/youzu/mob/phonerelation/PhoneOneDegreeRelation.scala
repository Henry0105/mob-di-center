package com.youzu.mob.phonerelation

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

/** 生成GA联系人一度正反关系表 */
object PhoneOneDegreeRelation {
  var day = ""

  def main(args: Array[String]): Unit = {
    day = args(0)

    /** 初始化spark上下文环境 */
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("graph")
      .getOrCreate()

    /** 并行度 */
    val partitionNum = 2501
    /** 加载数据 */
    val edgeRdd = loadDate(spark)
      .repartition(partitionNum)
      .persist(StorageLevel.DISK_ONLY)
    /** 聚合中心点 */
    val oneDegreeAll: RDD[(String, (String, String, String, String))] = positiveAndNegetiveAgg(edgeRdd, partitionNum)
      .persist(StorageLevel.DISK_ONLY)

    oneDegreeSearch(oneDegreeAll, spark)
  }

  /**
   * 从数据源读取数据
   *
   * @param spark spark
   * @return 通讯录数据rdd
   */
  def loadDate(spark: SparkSession): RDD[(String, String, String)] = {
    val inputPath = s"/user/hive/warehouse/rp_mobdi_app.db/rp_gapoi_myphone_phone/day=$day"
    val fileType = "lzo"
    val delimiter = "\\u0001"
    var rdd: RDD[(String, String, String)] = null
    if ("txt,lzo".contains(fileType)) {
      rdd = spark.read.textFile(inputPath).rdd.map(line => {
        val cols = line.split(delimiter)
        if (cols.length > 2) {
          (cols(0), cols(1), cols(2))
        } else {
          (null, null, null)
        }
      })
    } else if ("orc,parquet".contains(fileType)) {
      rdd = spark.read.format(fileType).load(inputPath).rdd.map(row => {
        if (row.size > 0 && row.size > 1) {
          (row.getString(0), row.getString(1), row.getString(2))
        } else {
          (null, null, null)
        }
      })
    }
    rdd.filter(tuple => {
      tuple._1 != null &&
        tuple._2 != null &&
        tuple._1.trim.length >= 10 &&
        !tuple._1.startsWith("0008600400") &&
        tuple._2.trim.length >= 10 &&
        !tuple._2.startsWith("0008600400") &&
        tuple._3 != null
    })
  }

  /**
   * 中心点聚合
   *
   * @param edgeRdd      通讯录数据rdd
   * @param partitionNum 并行度
   * @return 联系人一度关系rdd
   */
  def positiveAndNegetiveAgg(edgeRdd: RDD[(String, String, String)], partitionNum: Int): RDD[(String, (String, String, String, String))] = {
    val positiveEdge = edgeRdd.map(tuple => (tuple._1, (tuple._2, "p", tuple._3)))
    val negtiveEdge = edgeRdd.map(tuple => (tuple._2, (tuple._1, "n", tuple._3)))

    val oneDegreeSearch = positiveEdge.union(negtiveEdge)
      .aggregateByKey(List[(String, String, String)](), new HashPartitioner(partitionNum))(
        (list, edge) => list :+ edge,
        (list1, list2) => list1 ::: list2
      )
      .mapValues(truples => {

        val negtivemap = truples
          .filter(node_n => "n".equals(node_n._2))
          .map(node_n => (node_n._1, node_n._3))
          .toMap

        val negtive = negtivemap.keys.mkString("|")
        val negtivedate = negtivemap.values.mkString("|")

        val positivemap = truples
          .filter(node_p => "p".equals(node_p._2))
          .map(node_p => (node_p._1, node_p._3))
          .toMap

        val positive = positivemap.keys.mkString("|")
        val positivedate = positivemap.values.mkString("|")

        (positive, negtive, positivedate, negtivedate)
      })
    oneDegreeSearch
  }

  /**
   * 一度正反关系
   *
   * @param oneDegreeAll 联系人一度关系rdd
   * @param spark        spark
   */
  def oneDegreeSearch(oneDegreeAll: RDD[(String, (String, String, String, String))], spark: SparkSession): Unit = {
    val schemaList = Array[String]("phone", "p_rel_list", "n_rel_list", "p_rel_date_list", "n_rel_date_list")
    val schema = StructType(
      schemaList.map(fieldName => StructField(fieldName, StringType))
    )
    val rdd = oneDegreeAll.map(tuple => Row(tuple._1, tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4))
      .repartition(301)
    spark.createDataFrame(rdd, schema).createOrReplaceTempView("phone_onedegree_rel_tmp")
    spark.sql(
      s"""
         |insert overwrite table rp_mobdi_app.phone_onedegree_rel partition(day=$day)
         |select phone,p_rel_list,n_rel_list,p_rel_date_list,n_rel_date_list
         |from phone_onedegree_rel_tmp
        """.stripMargin)
  }
}
