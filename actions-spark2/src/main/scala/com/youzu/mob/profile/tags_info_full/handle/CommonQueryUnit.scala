package com.youzu.mob.profile.tags_info_full.handle

import com.youzu.mob.profile.tags_info_full.beans.{ProfileData, ProfileInfo, QueryUnitContext}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.SparkSession

/**
 * @author xlmeng
 */
case class CommonQueryUnit(override val cxt: QueryUnitContext, override val profiles: Array[ProfileInfo])
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    var tagsLikeSign = 0 // 0:不包含 1:tag_list 2:cate_list 3tag_list and catelist
    val ps1 = profiles.filter(p => {
      if (p.isTagListField) {
        tagsLikeSign = tagsLikeSign | 1
        true
      } else if (p.isCateListField) {
        tagsLikeSign = tagsLikeSign | 2
        true
      } else {
        false
      }
    })
    // 对taglist;7002_001先截取前面的,再分组
    val ps1Columns = ps1.groupBy(_.profileColumn.split(";")(0)).map { case (field, arr) =>
      processTaglistLikeFields(cxt.spark, arr, field)
    }.mkString(",")

    val ps2 = profiles.filterNot(_.hasTagListField)
    val ps2Columns = buildMapStringFromFields(ps2, kvSep)

    val columns = if (ps1.isEmpty) {
      // 没有tagsList/cateList
      s"concat_ws('$pairSep', $ps2Columns)"
    } else if (ps2Columns.isEmpty) {
      // 只有tagsList/cateList
      s"concat_ws('$pairSep', $ps1Columns)"
    } else {
      s"concat_ws('$pairSep', $ps2Columns, $ps1Columns)"
    }

    s"""
       |select ${hiveTable.key} as device
       |     , $columns as kv
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |     , $tagsLikeSign as tags_like_sign
       |from   ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
     """.stripMargin
  }

  // 处理taglist这样字段的udf
  def processTaglistLikeFields(spark: SparkSession, arr: Array[ProfileInfo], field: String): String = {
    val m = arr.map(p => p.profileColumn.split(";")(1) -> s"${p.fullVersionId}").toMap
    val mBC = spark.sparkContext.broadcast(m)
    val fn = s"process_${field}_${RandomStringUtils.randomAlphanumeric(5)}"

    spark.udf.register(s"$fn", (f: String) => {
      if (f == null) null else {
        val tmp = f.split("=")
        if (tmp.length <= 1) null else {
          val pairs = tmp(0).split(",").zip(tmp(1).split(",")).flatMap { case (tagId, v) =>
            mBC.value.get(tagId).map(fullId => s"$fullId$kvSep$v")
          }
          if (pairs.isEmpty) null else pairs.mkString(pairSep)
        }
      }
    })

    s"$fn($field)"
  }

  // concat(kvSep, gender, 1), concat(kvSep, agebin, 3)
  // concat(1_1000, kvSep, cast(gender_cl as string),concat(2_1000, kvSep, cast(agebin_cl as string)
  def buildMapStringFromFields[T <: ProfileData](arr: Array[T], kvSep: String): String = {
    arr.map(_.columnClause(kvSep)).mkString(",")
  }

}