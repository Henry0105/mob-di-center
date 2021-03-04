package com.youzu.mob.profile.tags_info_full.handle

import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext}
import com.youzu.mob.profile.tags_info_full.enums.PartitionType

/**
 * @author xlmeng
 */
object QueryUnitFactory {

  def createQueryUnit(cxt: QueryUnitContext, profiles: Array[ProfileInfo], isConfidence: Boolean = false):
  Seq[QueryUnit] = {
    if (isConfidence) {
      createConfidenceQueryUnit(cxt, profiles)
    } else {
      createTagsQueryUnit(cxt, profiles)
    }
  }

  /** 创建置信度的查询单元 */
  private def createConfidenceQueryUnit(cxt: QueryUnitContext, profiles: Array[ProfileInfo]): Seq[QueryUnit] = {
    profiles.groupBy(p => (p.fullTableName, p.period)).map {
      case (_, ps) =>
        ConfidenceQueryUnit(cxt, ps)
    }.toSeq
  }

  /** 创建标签的查询单元 */
  private def createTagsQueryUnit(cxt: QueryUnitContext, profiles: Array[ProfileInfo]): Seq[QueryUnit] = {
    profiles.groupBy(p => (p.fullTableName, p.period)).values.zipWithIndex.flatMap { case (ps, i) =>
      // 拼接成查询标签的sql语句
      if (ps.head.isTimewindowTable) {
        ps.groupBy(_.flagTimewindow).values.zipWithIndex.map { case (ps2, j) =>
          PartitionType(ps2.head.partitionType) match {
            case PartitionType.feature => OnlyFeatureQueryUnit(cxt, ps2)
            case PartitionType.timewindowFlagFeature => TimewindowFlagFeatureQueryUnit(cxt, ps2, s"${i}_$j")
            case _ => OtherTimewindowQueryUnit(cxt, ps2)
          }
        }
      } else {
        Seq(CommonQueryUnit(cxt, ps))
      }
    }.toSeq
  }


}
