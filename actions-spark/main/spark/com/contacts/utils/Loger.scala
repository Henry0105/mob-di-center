package com.contacts.utils

import com.contacts.common.Constant
import org.slf4j.{LoggerFactory, Logger}

trait Loger {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def log_info(msg: String, e: Throwable): Unit = LOGGER.info(Constant.STRING.MY_LOG_PREFIX + "\t" + msg, e)

  def log_info(msg: String): Unit = LOGGER.info(Constant.STRING.MY_LOG_PREFIX + "\t" + msg)

  def log_debug(msg: String, e: Throwable): Unit = LOGGER.debug(Constant.STRING.MY_LOG_PREFIX + "\t" + msg, e)

  def log_debug(msg: String): Unit = LOGGER.debug(Constant.STRING.MY_LOG_PREFIX + "\t" + msg)

  def log_error(msg: String, e: Throwable): Unit = LOGGER.error(Constant.STRING.MY_LOG_PREFIX + "\t" + msg, e)

  def log_error(msg: String): Unit = LOGGER.error(Constant.STRING.MY_LOG_PREFIX + "\t" + msg)

  def log_warn(msg: String, e: Throwable): Unit = LOGGER.warn(Constant.STRING.MY_LOG_PREFIX + "\t" + msg, e)

  def log_warn(msg: String): Unit = LOGGER.warn(Constant.STRING.MY_LOG_PREFIX + "\t" + msg)
}
