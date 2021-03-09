package com.mob.appgo.common.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateTimeUtil {
  final val YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss"
  final val YYYYMMDDHHMMSS = "yyyyMMddHHmmss"
  final val YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm"
  final val YYYYMMDDHHMM = "yyyyMMddHHmm"
  final val YYYY_MM_DD = "yyyy-MM-dd"

  final val YYYY_MM_DD2 = "yyyy/MM/dd"
  final val YYYYMMDD = "yyyyMMdd"
  final val YYYY = "yyyy"
  final val MM = "MM"
  final val DD = "dd"

  final val formats = Array(
    YYYY_MM_DD_HH_MM_SS,
    YYYYMMDDHHMMSS,
    YYYY_MM_DD_HH_MM,
    YYYYMMDDHHMM,
    YYYY_MM_DD,
    YYYY_MM_DD2,
    YYYYMMDD,
    YYYY,
    MM,
    DD
  )

  def getFormatDateTime(timestamp: Long): String = {
    getFormatDateTime(new Date(timestamp))
  }

  def getFormatDateTime(date: Date): String = {
    getFormatDateTime(date, YYYY_MM_DD_HH_MM_SS)
  }

  def getFormatDateTime(date: Date, format: String): String = {
    val dtFormatdB = new SimpleDateFormat(format)
    dtFormatdB.format(date)
  }

  def getFormatDateTime(date: String, format: String): String = {
    try {
      var flag = false
      var i = 0
      while (!flag && i < formats.length) {
        try {
          new SimpleDateFormat(formats(i)).parse(date)
          flag = true
        } catch {
          case _: Throwable => flag = false
        }
        i += 1
      }
      getFormatDateTime(new SimpleDateFormat(formats(i - 1)).parse(date), format)
    } catch {
      case _: Throwable => date
    }
  }

  def getFormatDateTime(date: String, originalFormat: String, desFormat: String): String = {
    getFormatDateTime(new SimpleDateFormat(originalFormat).parse(date), desFormat)
  }

  def main(args: Array[String]) {
    println(getFormatDateTime(new Date(), YYYYMMDD))
    println(getFormatDateTime("20160927", YYYY_MM_DD2))
    println(getFormatDateTime("20160927", YYYYMMDD, YYYY_MM_DD2))
  }
}
