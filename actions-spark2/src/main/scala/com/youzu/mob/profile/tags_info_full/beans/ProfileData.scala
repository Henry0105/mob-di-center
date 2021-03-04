package com.youzu.mob.profile.tags_info_full.beans

import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper
import org.apache.spark.sql.SparkSession

class ProfileData(
                   val profileId: Int,
                   val profileVersionId: Int,
                   val profileDatabase: String,
                   val profileTable: String,
                   val profileColumn: String,
                   val hasDatePartition: Int = 1,
                   val profileDataType: String = "string"
                 ) extends Serializable {
  def key(spark: SparkSession): String = {
    val fields = spark.table(fullTableName).schema.fieldNames
    if (fields.contains("device")) {
      "device"
    } else if (fields.contains("device_id")) {
      "device_id"
    } else if (fields.contains("deviceid")) {
      "deviceid"
    } else {
      "id"
    }
  }

  def hasTagListField: Boolean = profileColumn.contains(";") && !profileColumn.contains("=")
  def isTagListField: Boolean = profileColumn.contains("tag_list")
  def isCateListField: Boolean = profileColumn.contains("catelist")

  def fullVersionId: String = s"${profileId}_$profileVersionId"

  def fullTableName: String = s"$profileDatabase.$profileTable"

  def isPartitionTable: Boolean = 0 != hasDatePartition

  def columnClause(kvSep: String): String = {
    val arr = profileColumn.split(";").map(_.trim)
    s"concat('$fullVersionId', '$kvSep', ${TagsGeneratorHelper.valueToStr(profileDataType, arr(0))})"
  }

  // 如果是线上/线下标签,拿到对应的 flag=7 and timewindow='40'
  // v3表为: cnt;feature='5444_1000'
  def flagTimewindow: String = {
    if (profileColumn.contains("flag=")) {
      val flag = """flag=(\d+)""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      val timeWindow = """timewindow='(\d+)'""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      s"flag=$flag and timewindow='$timeWindow'"
    } else if (profileColumn.contains("feature=")) {
      val feature = """feature='(.+)'""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      s"feature='$feature'"
    } else {
      throw new Exception("标签配置有问题: " + profileColumn)
    }
  }
}
