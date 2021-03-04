package com.youzu.mob.dailyrun

import java.io.FileInputStream
import java.util.Properties

object ConfigurationManager {
  private[this] val prop = new Properties()

  def init(filePath: String): Unit = {
    //    val fs = FileSystem.get(URI.create(filePath), new Configuration())
    //    val in = fs.open(new Path(filePath))
    try {
      prop.load(new FileInputStream(filePath))
      //      val keyset = prop.keySet().toArray()
      //      keyset.foreach(t=>println(t+" "+prop.getProperty(t.toString)))
    } catch {
      case e: Exception => throw new RuntimeException("ConfigurationManager error.", e)
    }
  }

  def checkData(tagSeq: Seq[String]): Unit = {
    for (tag <- tagSeq) {
      if (getString("profile." + tag + ".data") == null || getString("profile." + tag + ".type") == null) {
        throw new RuntimeException("配置文件校验失败：profile [" + tag + "] info check failed in property file.")
      }
    }
  }


  def getTagSeq: Seq[String] = {
    var tagSeq: Seq[String] = Seq()
    val names = prop.propertyNames()
    while (names.hasMoreElements) {
      val ele = names.nextElement().toString
      if (ele.startsWith("profile.") && ele.endsWith(".data") && ele.split("\\.").length >= 3) {
        val tagName = ele.split("\\.")(1)
        tagSeq = tagSeq :+ tagName
      }
    }
    tagSeq
  }

  def getReviseMap: Map[String, String] = {
    var reviseMap: Map[String, String] = Map()
    val names = prop.propertyNames()
    while (names.hasMoreElements) {
      val ele = names.nextElement().toString
      if (ele.startsWith("revise.") && ele.endsWith(".rule") && ele.split("\\.").length >= 3) {
        val reviseName = ele.split("\\.")(1)
        val reviseRule = getString("revise." + reviseName + ".rule")
        if (reviseRule != null) {
          reviseMap += (reviseName -> reviseRule)
        }
      }
    }
    reviseMap
  }


  def getString: String => String = (key: String) => {
    prop.getProperty(key)
  }

  def getInt: String => Int = (key: String) => {
    getString(key).toInt
  }

  def getBoolean: String => Boolean = (key: String) => {
    getString(key).toBoolean
  }

  def getLong: String => Long = (key: String) => {
    getString(key).toLong
  }

}
