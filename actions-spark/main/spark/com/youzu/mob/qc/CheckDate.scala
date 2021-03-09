package com.youzu.mob.qc

import com.mail.MailSender
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object CheckDate {
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val email = args(1)
    val flag = args(2)
    val sparkConf = new SparkConf().setAppName(s"CheckDate_${day}")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val success = s"${flag}/${day}_SUCCESS"
    val path = new Path(success)
    if (hdfs.exists(path)) {
      hdfs.delete(path, false)
    }

    val install_app_info = hiveContext
      .sql(s"select count(*) as cnt from dw_sdk_log.log_device_install_app_all_info where day=${day}")
    val install = install_app_info.map(r => {
      var isRun = true
      val cnt = r.getAs[Long]("cnt").toFloat
      val all = 400000000f
      val percent = cnt / all
      if (percent < 0.7) {
        MailSender.sendMail("Error!(log_device_install_app_all_info) There is a problem with the data in this table ",
          s"""
             |The partition is ${day} <br>
             |${day}_cnt=${cnt},avg=${all},cnt/all=${percent}""".stripMargin, s"${email}")
        isRun = false
      }
      (isRun)
    })

    val install_app_incr_info = hiveContext.
      sql(s"select count(*) as cnt from dw_sdk_log.log_device_install_app_incr_info where day=${day}")
    val incr = install_app_incr_info.map(r => {
      val cnt = r.getAs[Long]("cnt").toFloat
      val all = 65669903f
      val percent = cnt / all
      var isRun = true
      if (percent < 0.7) {
        MailSender.sendMail(
          "Error!(log_device_install_app_incr_info) There is a problem with the data in this table ",
          s"""
             |The partition is ${day} <br>
             |${day}_cnt=${cnt},avg=${all},cnt/all=${percent}""".stripMargin, s"${email}")
        isRun = false
      }
      (isRun)
    })
    val unstall_app_info = hiveContext.
      sql(s"select count(*) as cnt from dw_sdk_log.log_device_unstall_app_info where day=${day}")
    val unstall = unstall_app_info.map(r => {
      var isRun = true
      val cnt = r.getAs[Long]("cnt").toFloat
      val all = 85461037f
      val percent = cnt / all
      if (percent < 0.7) {
        MailSender.sendMail(
          "Error!(log_device_unstall_app_info) There is a problem with the data in this table ",
          s"""
             |The partition is ${day} <br>
             |${day}_cnt=${cnt},avg=${all},cnt/all=${percent}""".stripMargin, s"${email}")
        isRun = false
      }
      (isRun)
    })
    if (install.first() && incr.first() && unstall.first()) {
      if (!hdfs.exists(path)) {
        hdfs.mkdirs(path)
      }
    }
    val app_runtimes = hiveContext.sql(s"select count(*) as cnt from dw_sdk_log.device_app_runtimes where day=${day}")
    app_runtimes.foreach(r => {
      val cnt = r.getAs[Long]("cnt").toFloat
      val all = 915543046f
      val percent = cnt / all
      if (percent < 0.7) {
        MailSender.sendMail(
          "warning!(device_app_runtimes) There is a problem with the data in this table ",
          s"""
             |The partition is ${day} <br>
             |${day}_cnt=${cnt},avg=${all},cnt/all=${percent}""".stripMargin, s"${email}")
      }
    })

    val log_device_info = hiveContext.sql(s"select count(*) as cnt from dw_sdk_log.log_device_info where dt=${day}")
    log_device_info.foreach(r => {
      val cnt = r.getAs[Long]("cnt").toFloat
      val all = 27194678f
      val percent = cnt / all
      if (percent < 0.7) {
        MailSender.sendMail(
          "warning!(log_device_info) There is a problem with the data in this table ",
          s"""
             |The partition is ${day} <br>
             |${day}_cnt=${cnt},avg=${all},cnt/all=${percent}""".stripMargin, s"${email}")
      }
    })

    val log_device_info_jh = hiveContext
      .sql(s"select count(*) as cnt from dw_sdk_log.log_device_info_jh where dt=${day}")
    log_device_info_jh.foreach(r => {
      val cnt = r.getAs[Long]("cnt").toFloat
      val all = 43383589f
      val percent = cnt / all
      if (percent < 0.7) {
        MailSender.sendMail("warning!(log_device_info_jh) There is a problem with the data in this table ",
          s"""
             |The partition is ${day} <br>
             |${day}_cnt=${cnt},avg=${all},cnt/all=${percent}""".stripMargin, s"${email}")
      }
    })

  }
}
