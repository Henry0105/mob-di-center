package com.youzu.mob.utils

import com.typesafe.config.ConfigFactory

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties
import scala.util.{Failure, Success, Try}

object PropUtils {

  private val propList = Array("application.properties", "hive_db_tb_master.properties",
    "hive_db_tb_mobdi_mapping.properties", "hive_db_tb_report.properties",
    "hive_db_tb_sdk_mapping.properties", "hive_db_tb_topic.properties"
  )

//  lazy private val prop = propList.map(ConfigFactory.load).reduce((a, b) => a.withFallback(b))

  def getProperty(key: String): String = prop.getProperty(key)

  // 需要外部 赋值 MOBDI_HOME
  lazy val prop = {
    val prop = new Properties()
    Try {
      propList
        .foreach(propFile => {
          val configPath = s"${System.getenv("MOBDI_HOME")}/conf/${propFile}"
          val file = new File(configPath)
          val propIn = if (file.exists()) {
            //  yarn-client模式加载外部资源文件
            new InputStreamReader(new FileInputStream(file))
          } else if (new File(propFile).exists()) {
            // yarn-cluster模式加载外部资源文件
            new InputStreamReader(new FileInputStream(new File(propFile)))
          } else {
            // 本地idea测试
            new InputStreamReader(new FileInputStream(new File(
              "./" + propFile)))
          }
          prop.load(propIn)
          propIn.close()
        })
    } match {
      case Success(_) =>
      case Failure(e) =>
    }
    prop
  }
}
