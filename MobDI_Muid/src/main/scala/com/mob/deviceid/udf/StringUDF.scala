package com.mob.deviceid.udf

import org.apache.commons.lang3.StringUtils

class StringUDF {
  def isBlank(s: String): Boolean = {
    StringUtils.isBlank(s)
  }
  def isNotBlank(s: String): Boolean = {
    StringUtils.isNotBlank(s)
  }
}
