package com.mob.appgo.common.utils

import org.slf4j.LoggerFactory

trait Logger {
  val LOGGER: org.slf4j.Logger = LoggerFactory.getLogger(getClass)

  def log_info(msg: String, e: Throwable): Unit = LOGGER.info(msg, e)

  def log_info(msg: String): Unit = LOGGER.info(msg)

  def log_debug(msg: String, e: Throwable): Unit = LOGGER.debug( msg, e)

  def log_debug(msg: String): Unit = LOGGER.debug(msg)

  def log_error(msg: String, e: Throwable): Unit = LOGGER.error( msg, e)

  def log_error(msg: String): Unit = LOGGER.error( msg)

  def log_warn(msg: String, e: Throwable): Unit = LOGGER.warn( msg, e)

  def log_warn(msg: String): Unit = LOGGER.warn(msg)
}
