package com.youzu.mob.profile.tags_info_full.handle

import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext}
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper

/**
 * @author xlmeng
 */
case class OtherTimewindowQueryUnit(override val cxt: QueryUnitContext, override val profiles: Array[ProfileInfo])
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    val fieldName = profile.profileColumn.split(";").map(_.trim).head
    val dataType = profile.profileDataType
    s"""
       |select ${hiveTable.key} as device
       |  , concat('${profile.fullVersionId}', '$kvSep', ${TagsGeneratorHelper.valueToStr(dataType, fieldName)}) as kv
       |  , ${hiveTable.updateTimeClause(profile)} as update_time
       |  , null as tags_like_sign
       |from  ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
     """.stripMargin
  }
}
