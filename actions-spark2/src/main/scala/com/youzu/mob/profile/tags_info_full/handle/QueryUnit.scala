package com.youzu.mob.profile.tags_info_full.handle


import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext}
import com.youzu.mob.profile.tags_info_full.enums.{PeriodEnum, UpdateType}
import com.youzu.mob.profile.tags_info_full.helper.{HiveTable, TagsDateProcess, TagsGeneratorHelper}
import org.apache.commons.lang3.StringUtils

/**
 * @author xlmeng
 */
abstract class QueryUnit(val cxt: QueryUnitContext, val profiles: Array[ProfileInfo]) extends Serializable {

  protected val kvSep: String = TagsGeneratorHelper.kvSep
  protected val pSep: String = TagsGeneratorHelper.pSep
  protected val pairSep: String = TagsGeneratorHelper.pairSep
  val profile: ProfileInfo = profiles.head
  protected lazy val hiveTable: HiveTable = buildHiveTableFromProfiles()
  lazy val msg = s"${profile.period}_${profile.periodDay}_${profile.profileColumn}"

  def query(): String

  def check(): (String, Boolean, String) = {
    // 非分区表和月更新表始终为true
    if (profile.isPartitionTable && PeriodEnum.withName(profile.period.toLowerCase()) != PeriodEnum.month) {
      val part = getAssignPartition()
      (profile.fullTableName, StringUtils.isNoneBlank(part), msg)
    } else {
      (profile.fullTableName, true, msg)
    }
  }

  protected def buildHiveTableFromProfiles(): HiveTable = {
    val key = profile.key(cxt.spark)
    val partitionClause = if (profile.isPartitionTable) {
      val part = getAssignPartition()
      if (StringUtils.isNotBlank(part)) part else "false"
    } else {
      "true"
    }

    val updateTimeClause = getUpdateTime()

    val whereClause =
      s"""
         |where (${profiles.map(p => s"(${p.whereClause()})").toSet.mkString(" or ")})
         |  and $partitionClause
         |  and ${if (cxt.full) "true" else updateTimeClause}
         |  and ${sampleClause(cxt.sample)}
         """.stripMargin

    HiveTable(profile.profileDatabase, profile.profileTable, key,
      profile.isPartitionTable, whereClause, partitionClause)
  }

  /**
   * 分区目前一共4类:
   * 1: day=20200623/timewindow=14/flag=2, day=20200623/timewindow=14/flag=3, day=20200623/timewindow=7/flag=2
   * 单独处理,[[getDayByDetailPartition]]
   * 2: day=20200627/feature=5698_1000, day=20200627/feature=5699_1000, day=20200627/feature=5701_1000
   * 3: day=20200623/timewindow=14, day=20200623/timewindow=30, day=20200623/timewindow=7
   * 2个分区字段，通过非day字段来取出day字段 [[getDayByDetailPartition()]]
   * 4: day=20200601
   * 直接取
   *
   * 日更新:首次全量生成和增量逻辑一致
   * 周更新:增量取本周指定日期，首次全量生成 如果本周指定日期已经生成取本周，否则取上周
   * 月更新:取小于任务日期的最新未更新日期
   */
  protected def getAssignPartition(): String = {
    val parts = cxt.tbManager.getPartitions(profile.fullTableName)
    val dataDate = TagsDateProcess.getDataDate(this)
    val assignParts = parts.foldLeft(List.empty[String])((xs, x) =>
      if ("""\d{8}""".r.findFirstMatchIn(x).get.matched == dataDate) x :: xs else xs
    ).filterNot(_.contains("monthly_bak"))

    val detailPartition = profile.getDetailPartition
    if (assignParts.nonEmpty) {
      if (detailPartition.nonEmpty) {
        getDayByDetailPartition(assignParts, profile, detailPartition)
      } else {
        assignParts.head
      }
    } else ""
  }

  private def getDayByDetailPartition(parts: Seq[String], profileInfo: ProfileInfo,
                                      detailPartition: Seq[String]): String = {
    println(s"filter ${profileInfo.fullTableName} with ${detailPartition.mkString(" and ")}")
    val fdt = parts.filter { p =>
      val arr = p.split("/")
      detailPartition.forall(arr.contains(_))
    }
    if (fdt.nonEmpty) {
      /** 对于timewindow 需要只取出day字段 */
      """day=(\d+)""".r.findFirstMatchIn(fdt.last).get.matched
    } else {
      ""
    }
  }

  // 2种情况,全量表需要取update_time(目前都是日更新),增量表直接走分区筛选
  private def getUpdateTime(): String = {
    if (UpdateType.isFull(profile.updateType)) {
      profile.profileTable match {
        case "device_profile_label_full_par" => s" update_time = '${cxt.day}' "
        case "rp_device_profile_full_with_confidence" => s" update_time = '${cxt.day}' "
        case _ => s" processtime = '${cxt.day}' "
      }
    } else {
      " true "
    }
  }

  // sample=true 是测试使用
  protected def sampleClause(sample: Boolean, key: String = "device"): String = {
    val randomType = (Math.random() * 100000).toInt
    if (sample) {
      s"hash($key)%1000000=$randomType"
    } else {
      "true"
    }
  }

}