package com.mob.appgo.etl.control

import com.mob.appgo.common.mysql.{DBInfo, DBUtils, DBUtilsInter}
import com.mob.appgo.common.utils.{DateTimeUtil, FileOperateUtils, HDFSUtils, Logger}
import com.mob.appgo.etl.common.{ChannelOption, PhaseOption}

import scala.collection.mutable
import scala.util.control.Breaks

object CrawlerBatchControl extends Logger {
  System.setProperty("HADOOP_USER_NAME", "dba")
  final val ChannelPhaseArr = Array(
    (ChannelOption.GoogleCATE, PhaseOption.DETAIL),
    (ChannelOption.GoogleCATE, PhaseOption.LIST),
    (ChannelOption.GoogleCATE, PhaseOption.REVIEW),
    (ChannelOption.GoogleFEAT, PhaseOption.DETAIL),
    (ChannelOption.GoogleFEAT, PhaseOption.LIST),
    (ChannelOption.GoogleSEAR, PhaseOption.LIST),
    (ChannelOption.ItunesCATE, PhaseOption.DETAIL),
    (ChannelOption.ItunesCATE, PhaseOption.LIST),
    (ChannelOption.ItunesCATE, PhaseOption.REVIEW),
    (ChannelOption.ItunesFEAT, PhaseOption.DETAIL),
    (ChannelOption.ItunesFEAT, PhaseOption.LIST),
    (ChannelOption.ItunesSEAR, PhaseOption.LIST)
  )

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.out.println("参数不正确，请重新输入")
      System.exit(999)
    }
    val day: String = args(0)

    val loop = new Breaks

    val appgoCrawlerDbInfo: DBInfo = new DBInfo
    appgoCrawlerDbInfo.setDriver("appgocrawler.jdbc.driverClassName")
    appgoCrawlerDbInfo.setUrl("appgocrawler.jdbc.url")
    appgoCrawlerDbInfo.setUserName("appgocrawler.jdbc.username")
    appgoCrawlerDbInfo.setPwd("appgocrawler.jdbc.password")
    val appgoCrawlerJdbc: DBUtilsInter = new DBUtils(appgoCrawlerDbInfo)

    val dailyRunDbInfo: DBInfo = new DBInfo
    dailyRunDbInfo.setDriver("dailyrun.jdbc.driverClassName")
    dailyRunDbInfo.setUrl("dailyrun.jdbc.url")
    dailyRunDbInfo.setUserName("dailyrun.jdbc.username")
    dailyRunDbInfo.setPwd("dailyrun.jdbc.password")
    val dailyRunJdbc: DBUtilsInter = new DBUtils(dailyRunDbInfo)


    ChannelPhaseArr.foreach(f = channelPhase => {
      val channel = channelPhase._1.toString
      val phase = channelPhase._2.toString
      val date = DateTimeUtil.getFormatDateTime(day, DateTimeUtil.YYYYMMDD, DateTimeUtil.YYYYMMDD)
      val batchNumMax = if (channel.contains("Category") && phase.equals("list_page_phase")) 4 else 1
      (1 to batchNumMax).foreach { batchNum => exectBatch(channel)(phase)(date)(batchNum) }
    })

    def exectBatch(channel: String)(phase: String)(date: String)(batchNum: Int): Unit = {
      loop.breakable {
        log_info(s"开始判断CrawlerBatch中$date:$channel-${phase}第${batchNum}批次记录.")

        val dailyRunQuerySql = ControlUtils.genGetFlagQuery(channel, phase, batchNum, date)
        val getFlag = dailyRunJdbc.query(dailyRunQuerySql).apply(0).get("getFlag").getOrElse("").toString

        if (getFlag.toInt == 0) {
          log_info(s"CrawlerBatch中$date:$channel-${phase}第${batchNum}批次记录尚未获取到,并开始判断数据是否爬完.")

          val appgoCrawlDate = DateTimeUtil.getFormatDateTime(date, DateTimeUtil.YYYYMMDD, DateTimeUtil.YYYY_MM_DD)
          val workCntQuerySql = ControlUtils.genWorkCntQuery(channel, phase, batchNum, appgoCrawlDate)


          try {
            appgoCrawlerJdbc.query(workCntQuerySql).apply(0).get("workcnt").getOrElse("").toString
          } catch {
            case _: Throwable =>
              log_warn(s"JobHistory中$date:无$channel-${phase}第${batchNum}批次记录！中断判断！")
              loop.break()
          }


          val workCnt = appgoCrawlerJdbc.
            query(workCntQuerySql)
            .map(r => r.get("workcnt").getOrElse("").toString.toInt).min

          val cntQuerySql = ControlUtils.genCntQuery(channel, phase, batchNum.toInt, appgoCrawlDate)
          val count = appgoCrawlerJdbc.query(cntQuerySql).apply(0).get("cnt").getOrElse("").toInt

          log_info(s"JobHistory中$date:$channel-${phase}第${batchNum}批次记录中，WorkCnt：$workCnt，Count：$count")

          if (workCnt == count && count != 0 && workCnt != 0) {
            log_info(s"JobHistory中$date:$channel-${phase}第${batchNum}批次爬取完成，开始执行获取操作.")

            val appgoCrawlerArrMapSql: String = ControlUtils.
              genAppgoCrawlerQuery(channel, phase, batchNum.toInt, appgoCrawlDate)
            val appgoCrawlerArrMap: Array[mutable.HashMap[String, String]] = appgoCrawlerJdbc.
              query(appgoCrawlerArrMapSql)

            val appgoCrawlerBeginTime = appgoCrawlerArrMap.map(
              appgoCrawlerMap => appgoCrawlerMap.get("begintime").getOrElse("")).min
            val appgoCrawlerEndTime = appgoCrawlerArrMap.map(
              appgoCrawlerMap => appgoCrawlerMap.get("finishtime").getOrElse("")).max
            appgoCrawlerArrMap.foreach(f = appgoCrawlerMap => {
              val worknode = appgoCrawlerMap.get("worknode").getOrElse("")
              val datapath = appgoCrawlerMap.get("datapath").getOrElse("")
              val crawlBegin = appgoCrawlerBeginTime
              val crawlEnd = appgoCrawlerEndTime
              log_info("WorkNode: " + worknode)
              log_info("DataPath: " + datapath)
              log_info("CrawlBegin: " + crawlBegin)
              log_info("CrawlEnd: " + crawlEnd)

              val dailyRunUpdateGetFlagSql = ControlUtils.
                genGetFlagUpdate(channel, phase, batchNum.toInt, crawlBegin, crawlEnd, date)

              if (!ControlUtils.checkLocalFileExists(worknode, datapath, batchNum, date)) {
                log_info("Local File Does Not Exist!")
                val remoteFilePath = ControlUtils.makeRemoteFilePath(worknode, datapath, batchNum)
                val localDestFilePath = ControlUtils.makeLocalDestFile(worknode, datapath, batchNum, date)
                log_info("RemoteFilePath: " + remoteFilePath)
                log_info("LocalDestFilePath: " + localDestFilePath)
                FileOperateUtils.copyFile(remoteFilePath, localDestFilePath, overwrite = true)
                if (!ControlUtils.checkHdfsFileExists(worknode, datapath, batchNum, date)) {
                  val localDestFile = ControlUtils.makeLocalDestFile(worknode, datapath, batchNum, date)
                  val hdfsDestFilePath = ControlUtils.makeHdfsFilePath(worknode, datapath, batchNum, date)
                  log_info("LocalDestFile: " + localDestFile)
                  log_info("HdfsDestFilePath: " + hdfsDestFilePath)
                  if (!HDFSUtils.isExists(hdfsDestFilePath)) {
                    log_info("Hdfs-Path does not exist.Start to make it and put the file on the hdfs.")
                    HDFSUtils.mkdir(hdfsDestFilePath)
                    HDFSUtils.uploadLocalFile2HDFS(localDestFile, hdfsDestFilePath)
                    if (ControlUtils.checkHdfsFileExists(worknode, datapath, batchNum, date)) {
                      log_info("Hdfs-File has been put successlly.")
                      dailyRunJdbc.executeBatch(dailyRunUpdateGetFlagSql)
                    }
                  } else {
                    log_info("Hdfs-Path do exist.Start to put the file on the hdfs.")
                    HDFSUtils.uploadLocalFile2HDFS(localDestFile, hdfsDestFilePath)
                    if (ControlUtils.checkHdfsFileExists(worknode, datapath, batchNum, date)) {
                      log_info("Hdfs-File has been put successlly.")
                      dailyRunJdbc.executeBatch(dailyRunUpdateGetFlagSql)
                    }
                  }
                }
              } else {
                log_info("Local-file do exist.Start to check the hdfs-file.")
                if (!ControlUtils.checkHdfsFileExists(worknode, datapath, batchNum, date)) {
                  val localDestFile = ControlUtils.makeLocalDestFile(worknode, datapath, batchNum, date)
                  val hdfsDestFilePath = ControlUtils.makeHdfsFilePath(worknode, datapath, batchNum, date)
                  log_info("LocalDestFile: " + localDestFile)
                  log_info("HdfsDestFilePath: " + hdfsDestFilePath)
                  if (!HDFSUtils.isExists(hdfsDestFilePath)) {
                    log_info("Hdfs-Path does not exist.Start to make it and put the file on the hdfs.")
                    HDFSUtils.mkdir(hdfsDestFilePath)
                    HDFSUtils.uploadLocalFile2HDFS(localDestFile, hdfsDestFilePath)
                    if (ControlUtils.checkHdfsFileExists(worknode, datapath, batchNum, date)) {
                      log_info("Hdfs-File: has been put successlly.")
                      dailyRunJdbc.executeBatch(dailyRunUpdateGetFlagSql)
                    }
                  } else {
                    log_info("Hdfs-Path do exist.Start to put the file on the hdfs.")
                    HDFSUtils.uploadLocalFile2HDFS(localDestFile, hdfsDestFilePath)
                    if (ControlUtils.checkHdfsFileExists(worknode, datapath, batchNum, date)) {
                      log_info("Hdfs-File has been put successlly.")
                      dailyRunJdbc.executeBatch(dailyRunUpdateGetFlagSql)
                    }
                  }
                }
              }
            })
          } else {
            log_info(s"JobHistory中$date:$channel-${phase}第${batchNum}批次尚未爬取完成.")
          }
        } else {
          log_info(s"CrawlerBatch中$date:$channel-${phase}第${batchNum}批次记录已经获取到.")
        }
      }
    }

    appgoCrawlerJdbc.close()
    dailyRunJdbc.close()
    System.exit(0)
  }
}
