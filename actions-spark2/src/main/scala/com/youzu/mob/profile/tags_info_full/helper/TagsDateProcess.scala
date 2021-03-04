package com.youzu.mob.profile.tags_info_full.helper

import java.time.{LocalDate, Year, YearMonth}

import com.youzu.mob.profile.tags_info_full.TagsGeneratorIncr
import com.youzu.mob.profile.tags_info_full.enums.PeriodEnum
import com.youzu.mob.profile.tags_info_full.handle.QueryUnit
import com.youzu.mob.utils.DateUtils.fmt
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper.sql
import com.youzu.mob.utils.DateUtils

import scala.collection.mutable

/**
 * @author xlmeng
 */
object TagsDateProcess {

  val curMonthPartitions: mutable.Map[(String, String), String] = mutable.Map.empty[(String, String), String]
  val preMonthPartitions: mutable.Map[(String, String), String] = mutable.Map.empty[(String, String), String]

  /** 数据生成时间 hive表的数据分区 */
  def getDataDate(qu: QueryUnit): String = {
    val day = qu.cxt.day
    val period = qu.profile.period
    val periodDay = qu.profile.periodDay
    if (qu.cxt.full) {
      getDataDateFullFirst(day, period, periodDay, qu)
    } else {
      getDataDateIncr(day, period, periodDay, qu)
    }
  }

  /**
   * 数据生成时间(增量)
   * 1.日更新: 数据生成时间
   * 2.周更新: 数据生成时间(本周的)
   * 3.月更新: 取现有表分区已经更新且全库标签表未更新的这个月和上个月约定日期的较大者
   *
   * @param day       任务日期
   * @param period    周期类型
   * @param periodDay 周期日期
   * @param qu        QueryUnit对象
   */
  def getDataDateIncr(day: String, period: String, periodDay: Int, qu: QueryUnit): String = {
    val date = LocalDate.parse(day, fmt)
    val value = (PeriodEnum.withName(period.toLowerCase()): @unchecked) match {
      case PeriodEnum.day => date
      case PeriodEnum.week => date.minusDays(date.getDayOfWeek.getValue - periodDay)
      case PeriodEnum.month =>
        val curDate = YearMonth.parse(day, fmt).atDay(periodDay)
        val preDate = curDate.minusMonths(1)
        val curDay = fmt.format(curDate)
        val preDay = fmt.format(preDate)
        val parts = qu.cxt.tbManager.getPartitions(qu.profile.fullTableName)
        val calDays = parts.map("""\d{8}""".r.findFirstMatchIn(_).get.matched)
          .filter(d => d == curDay || d == preDay)
        return if (calDays.isEmpty) "" else getUpdatedMonth(calDays.max, qu)
      case PeriodEnum.year => Year.parse(day, fmt).atDay(periodDay)
    }
    fmt.format(value)
  }

  /**
   * 数据生成时间(全量初次生成)
   * 1.日更新: 数据生成时间
   * 2.周更新: 任务日期 >= 本周的数据生成时间 : 本周的数据生成时间 ？ 上周的数据生成时间
   * 3.月更新: 采用上个月数据，如果当月数据存在，第二天会更新进来
   *
   * @param day       任务日期
   * @param period    周期类型
   * @param periodDay 周期日期
   */
  def getDataDateFullFirst(day: String, period: String, periodDay: Int, qu: QueryUnit): String = {
    val date = LocalDate.parse(day, fmt)
    val value = (PeriodEnum.withName(period.toLowerCase()): @unchecked) match {
      case PeriodEnum.day => date
      case PeriodEnum.week =>
        val calDay = date.minusDays(date.getDayOfWeek.getValue - periodDay)
        if (date.compareTo(calDay) >= 0) calDay else calDay.minusWeeks(1)
      case PeriodEnum.month =>
        val calDate = YearMonth.parse(day, fmt).atDay(periodDay).minusMonths(1)
        val calDay = fmt.format(calDate)
        return getUpdatedMonth(calDay, qu)
      case PeriodEnum.year => Year.parse(day, fmt).atDay(periodDay)
    }
    fmt.format(value)
  }

  private def getUpdatedMonth(calDay: String, qu: QueryUnit): String = {
    if (isUpdated(calDay, qu)) calDay else ""
  }

  /**
   * 判断月更新数据是否需要更新
   */
  private def isUpdated(calDay: String, qu: QueryUnit): Boolean = {
    val pday = DateUtils.minusDays(qu.cxt.day, 1)
    fillPreMonthPartitions(pday, qu)

    val detailPartition = qu.profile.getDetailPartition.mkString(" and ")
    val par = preMonthPartitions.get((qu.profile.fullTableName, detailPartition))
    if (par.isDefined && par.head == calDay) {
      false
    } else {
      curMonthPartitions.update((qu.profile.fullTableName, detailPartition), calDay)
      true
    }
  }

  /** 懒生成preMonthPartitions对象 */
  private def fillPreMonthPartitions(pday: String, qu: QueryUnit) {
    if (preMonthPartitions.isEmpty) {
      sql(
        qu.cxt.spark,
        s"""
           |select table_name, detail_partition, cur_partition
           |from ${TagsGeneratorIncr.monthUpdateTable}
           |where day = '$pday'
           |""".stripMargin)
        .collect()
        .foreach(r =>
          preMonthPartitions.put(
            (r.getAs[String]("table_name"), r.getAs[String]("detail_partition")),
            r.getAs[String]("cur_partition"))
        )
    }
  }

  /**
   * 数据处理时间
   * 1.日更新: 数据处理时间 = 数据生成时间
   * 2.周更新: 数据处理时间 = 数据生成时间
   * 3.月更新: 数据处理时间 = 每一天都处理
   */
  def getProcessDate(day: String, period: String, periodDay: Int): String = {
    val date = LocalDate.parse(day, fmt)
    val value = (PeriodEnum.withName(period.toLowerCase()): @unchecked) match {
      case PeriodEnum.day => date
      case PeriodEnum.week => date.minusDays(date.getDayOfWeek.getValue - periodDay)
      case PeriodEnum.month =>
        // YearMonth.parse(day, fmt).atDay(periodDay)
        date
      case PeriodEnum.year => Year.parse(day, fmt).atDay(periodDay)
    }
    fmt.format(value)
  }
}
