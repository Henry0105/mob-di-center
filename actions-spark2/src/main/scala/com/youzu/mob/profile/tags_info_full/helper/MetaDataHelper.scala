package com.youzu.mob.profile.tags_info_full.helper

import java.util.Properties

import com.youzu.mob.profile.tags_info_full.beans.ProfileInfo
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper._
import com.youzu.mob.utils.PropUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object MetaDataHelper {
  /** 单体标签mysql表 */
  val individualProfile = "t_individual_profile"
  /** 标签元信息表 */
  val profileMetadata = "t_profile_metadata"
  /** 标签分类表 */
  val profileCategory = "t_profile_category"
  /** 置信度表 */
  val profileConfidence = "t_profile_confidence"
  /** 输出标签表 */
  val labelOutput = "label_output"
  /** 输出标签画像表 */
  val labelOutputProfile = "label_output_profile"
  /** 标签版本表 */
  val profileVersion = "t_profile_version"
}

case class MetaDataHelper(spark: SparkSession, pre: String = "") {

  import MetaDataHelper._
  import spark.implicits._

  private val ip: String = PropUtils.getProperty(s"tag.mysql.ip$pre")
  private val port: Int = PropUtils.getProperty(s"tag.mysql.port$pre").toInt
  private val user: String = PropUtils.getProperty(s"tag.mysql.user$pre")
  private val pwd: String = PropUtils.getProperty(s"tag.mysql.password$pre")
  private val db: String = PropUtils.getProperty(s"tag.mysql.database$pre")
  private val url: String = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"

  val properties: Properties = new Properties()
  properties.setProperty("user", user)
  properties.setProperty("password", pwd)
  properties.setProperty("driver", "com.mysql.jdbc.Driver")

  createViewByMysql()

  private def createViewByMysql(): Unit = {
    Seq(individualProfile, profileConfidence, profileVersion, profileMetadata, profileCategory).foreach(tb =>
      spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
    )
    import org.apache.spark.sql.functions._
    /* 替换掉分区表 */
    val whenCol = col("profile_table") === "rp_device_profile_full_view"
    spark.table(individualProfile)
      .withColumn("has_date_partition", when(whenCol, 1).otherwise(col("has_date_partition")))
      .withColumn("profile_table", when(whenCol,
        "device_profile_label_full_par").otherwise(col("profile_table")))
      .createOrReplaceTempView(individualProfile)

    val whenCol2 = col("profile_table") === "rp_device_profile_full_with_confidence_view"
    spark.table(profileConfidence)
      .withColumn("profile_table", when(whenCol2,
        "rp_device_profile_full_with_confidence").otherwise(col("profile_table")))
      .createOrReplaceTempView(profileConfidence)
  }

  /**
   * 从mysql中读取对应标签信息
   */
  private def readFromMySQl(exclude: String): DataFrame = {
    val excludeCol = if (StringUtils.isBlank(exclude)) {
      ""
    } else {
      val excludeTables = exclude.split(",")
      excludeTables.map(table => s"a.profile_table <> '$table'").mkString("AND ", " AND ", "")
    }
    sql(
      spark,
      s"""
         |SELECT a.profile_id
         |     , a.profile_version_id
         |     , a.profile_database
         |     , a.profile_table
         |     , a.profile_column
         |     , a.has_date_partition
         |     , a.profile_datatype
         |     , a.period     --更新频率：day,week,month,year
         |     , a.period_day --频率对应的日期,周几,一个月第几天
         |     , a.update_type
         |     , a.partition_type
         |FROM   $individualProfile as a
         |INNER JOIN $profileVersion as b
         |ON b.is_avalable = 1 AND b.is_visible = 1
         |  AND a.profile_id = b.profile_id AND a.profile_version_id = b.profile_version_id
         |INNER JOIN $profileMetadata as d
         |ON a.profile_id = d.profile_id
         |INNER JOIN $profileCategory as c
         |ON d.profile_category_id = c.profile_category_id
         |WHERE a.profile_datatype IN ('int', 'string', 'boolean', 'double', 'bigint')
         |  AND (a.profile_table not like '%idfa%' AND a.profile_table not like '%ios%'
         |   AND a.profile_table not like '%imei%' AND a.profile_table not like '%mac%'
         |   AND a.profile_table not like '%phone%' AND a.profile_table not like '%serialno%'
         |   AND a.profile_table not like '%ifid%'
         |   AND a.profile_table not like '%ieid%' AND a.profile_table not like '%mcid%'
         |   AND a.profile_table not like '%pid%' AND a.profile_table not like '%snid%')
         |   $excludeCol
         |""".stripMargin)
  }

  def parseProfileInfo(df: DataFrame): Array[ProfileInfo] = {
    df.map(r => ProfileInfo(
      r.getAs[Int]("profile_id"),
      r.getAs[Int]("profile_version_id"),
      r.getAs[String]("profile_database"),
      r.getAs[String]("profile_table"),
      r.getAs[String]("profile_column"),
      r.getAs[Int]("has_date_partition"),
      r.getAs[String]("profile_datatype"),
      r.getAs[String]("period"),
      r.getAs[String]("period_day").toInt,
      r.getAs[String]("update_type"),
      r.getAs[String]("partition_type").toInt
    )).collect()
  }

  def getComputedProfiles(day: String, exclude: String, isFull: Boolean = false): Array[ProfileInfo] = {
    // 带有版本号 1_1000,2_1000,3_1000
    val tagsMetaDataDF = readFromMySQl(exclude)
    val profiles = parseProfileInfo(tagsMetaDataDF)

    if (isFull) {
      profiles
    } else {
      profiles.filter(p => day == TagsDateProcess.getProcessDate(day, p.period, p.periodDay))
    }
  }

  /**
   * 获取类似tag_list和catelist这样存储的所有的profile_id, version_id
   */
  def findTaglistLikeProfiles(prefix: String): Map[String, String] = {
    sql(
      spark,
      s"""
         |select profile_id, profile_version_id, profile_column
         |from $individualProfile
         |where profile_column like '$prefix%'
       """.stripMargin)
      .map { rs =>
        val profileId = rs.getAs[Int]("profile_id")
        val versionId = rs.getAs[Int]("profile_version_id")
        val id = rs.getAs[String]("profile_column").split(";")(1)
        (id, s"${profileId}_$versionId")
      }.collect().toMap
  }

  /**
   * 读取mysql中置信度信息
   */
  def getProfileConfidence(): Array[ProfileInfo] = {
    val metaDataDF = sql(spark,
      s"""
         |SELECT a.profile_id
         |     , a.profile_version_id
         |     , a.profile_database
         |     , a.profile_table
         |     , a.profile_column
         |     , 1 as has_date_partition
         |     , a.profile_datatype
         |     , b.period     --更新频率：day,week,month,year
         |     , b.period_day --频率对应的日期,周几,一个月第几天
         |     , b.update_type
         |     , b.partition_type
         |FROM $profileConfidence a
         |INNER JOIN $individualProfile b
         |ON b.os = 'Android' AND a.profile_id = b.profile_id AND a.profile_version_id = b.profile_version_id
      """.stripMargin)
    parseProfileInfo(metaDataDF)
  }

}