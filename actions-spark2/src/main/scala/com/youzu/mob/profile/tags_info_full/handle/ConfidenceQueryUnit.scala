package com.youzu.mob.profile.tags_info_full.handle

import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext}
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper

/**
 * @author xlmeng
 */
case class ConfidenceQueryUnit(override val cxt: QueryUnitContext, override val profiles: Array[ProfileInfo])
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    s"""
       |select ${hiveTable.key} as device
       |     , concat_ws('$pairSep', ${TagsGeneratorHelper.buildMapStringFromFields(profiles, kvSep)}) as confidence
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |     , null as tags_like_sign
       |from ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
        """.stripMargin
  }

}