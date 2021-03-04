package com.youzu.mob.profile.tags_info_full.helper

import com.youzu.mob.profile.tags_info_full.beans.ProfileInfo
import com.youzu.mob.profile.tags_info_full.enums.UpdateType

/**
 * @author xlmeng
 */
case class HiveTable(
                      database: String,
                      table: String,
                      key: String,
                      isPartitionTable: Boolean,
                      whereClause: String,
                      partitionClause: String
                    ) {

  def fullTableName: String = s"$database.$table"

  def updateTimeField(): String = {
    if (isPartitionTable) {
      val maybeMatch = """(\w+)=(\d+)""".r.findFirstMatchIn(partitionClause)
      if (maybeMatch.isDefined) maybeMatch.get.subgroups.head else "null"
    } else {
      "processtime"
    }
  }

  def updateTimeClause(profileInfo: ProfileInfo): String = {
    if (UpdateType.isFull(profileInfo.updateType)) {
      profileInfo.profileTable match {
        case "device_profile_label_full_par" => "processtime_all"
        case "rp_device_profile_full_with_confidence" => "processtime_all"
        case _ => "processtime"
      }
    } else {
      updateTimeField()
    }
  }
}
