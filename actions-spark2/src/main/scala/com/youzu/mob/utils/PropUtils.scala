package com.youzu.mob.utils

import java.io.{IOException, InputStreamReader}
import java.util.Properties

import org.slf4j.LoggerFactory

/**
 * @author juntao zhang
 */
object PropUtils {
  lazy private[this] val logger = LoggerFactory.getLogger(PropUtils.getClass)
  try {
    val jdbcIn = new InputStreamReader(PropUtils.getClass.getClassLoader
      .getResourceAsStream("application.properties"), "UTF-8")
    prop.load(jdbcIn)
    jdbcIn.close()
  } catch {
    case ex: IOException => logger.error(ex.getMessage, ex)
  }
  lazy private val prop: Properties = new Properties()

  def getProperty(key: String): String = prop.getProperty(key)
}
