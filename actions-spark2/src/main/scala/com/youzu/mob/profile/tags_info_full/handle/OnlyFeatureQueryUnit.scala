package com.youzu.mob.profile.tags_info_full.handle

import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext}
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper

/**
 * @author xlmeng
 */
case class OnlyFeatureQueryUnit(override val cxt: QueryUnitContext, override val profiles: Array[ProfileInfo])
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    // 这里处理只有feature字段的表 例如v3
    val mWithPid = TagsGeneratorHelper.getValue2IdMapping(profiles).toSeq
    val fieldName = profile.profileColumn.split(";").map(_.trim).head
    val dataType = profile.profileDataType
    s"""
       |select ${hiveTable.key} as device
       |     , concat('${mWithPid.head._2}', '$kvSep', ${TagsGeneratorHelper.valueToStr(dataType, fieldName)}) as kv
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |     , null as tags_like_sign
       |from ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
     """.stripMargin
  }

}