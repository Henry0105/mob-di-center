package com.youzu.mob.profile.tags_info_full.handle

import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext}
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper

/**
 * @author xlmeng
 */
case class TimewindowFlagFeatureQueryUnit(override val cxt: QueryUnitContext, override val profiles: Array[ProfileInfo],
                                          mock: String) extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    import cxt.spark.implicits._
    // e.g. "fin04_7_40" -> 1_1000
    val mWithPid = TagsGeneratorHelper.getValue2IdMapping(profiles).toSeq
    val fieldName = profile.profileColumn.split(";").map(_.trim).head
    val dataType = profile.profileDataType

    val featureMapping = s"feature_mapping_$mock"
    cxt.spark.createDataset(mWithPid)
      .toDF("feature_value", "feature_id")
      .createOrReplaceTempView(featureMapping)

    // 使用inner join来获取对应的id
    s"""
       |select /*+ BROADCASTJOIN(b) */ ${hiveTable.key} as device
       |     , concat(b.feature_id, '$kvSep', ${TagsGeneratorHelper.valueToStr(dataType, fieldName)}) as kv
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |     , null as tags_like_sign
       |from  ${hiveTable.fullTableName} as a
       |inner join $featureMapping as b
       |on a.feature = b.feature_value
       |${hiveTable.whereClause}
     """.stripMargin
  }

}