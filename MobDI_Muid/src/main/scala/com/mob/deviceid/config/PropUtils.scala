package com.mob.deviceid.config

import java.io.InputStreamReader
import java.util.Properties

import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class PropUtils(propFile: String) extends Serializable {

  private[this] lazy val logger: Logger = Logger.getLogger(this.getClass)
  private[this] lazy val prop: Properties = {
    val _prop = new Properties()
    Try {
      val propIn = new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream(propFile), "UTF-8")
      _prop.load(propIn)
      propIn.close()
    } match {
      case Success(_) =>
        logger.info(s"props loaded succeed from [$propFile], {${_prop}}")
        _prop
      case Failure(ex) =>
        logger.error(ex.getMessage, ex)
        throw new InterruptedException(ex.getMessage)
    }
  }

  def getProperty(key: String): String = prop.getProperty(key.trim)
  lazy val propsMap: mutable.Map[String, String] = prop.asScala

}
