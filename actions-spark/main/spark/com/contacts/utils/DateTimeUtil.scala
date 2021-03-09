package com.contacts.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateTimeUtil {
  final val TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

  def getFormatDateTime(timestamp: Long): String = {
    getFormatDateTime(new Date(timestamp))
  }

  def getFormatDateTime(date: Date): String = {
    getFormatDateTime(date, TIME_FORMAT)
  }

  def getFormatDateTime(date: Date, format: String): String = {
    val dtFormatdB = new SimpleDateFormat(format)
    dtFormatdB.format(date)
  }
}
