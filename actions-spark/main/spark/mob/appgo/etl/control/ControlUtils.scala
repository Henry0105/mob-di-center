package com.mob.appgo.etl.control

import java.io.File
import com.mob.appgo.common.utils.HDFSUtils

import scala.collection.mutable

object ControlUtils {
  def genGetFlagQuery(channel: String, phase: String, batchNum: Int, date: String): String = {
    new StringBuilder().append("select getFlag from crawlerbatch where ")
      .append(" channel='" + channel + "'")
      .append(" and phase='" + phase + "'")
      .append(" and batchNum=" + batchNum)
      .append(" and day=" + date).toString()
  }

  def genGetFlagUpdate(channel: String, phase: String, batchNum: Int,
    crawlBegin: String, crawlEnd: String, date: String): String = {
    new StringBuilder().append(
      s"""delete from crawlerbatch
          where channel='$channel'
          and phase='$phase'
          and batchNum=$batchNum
          and day=$date;
          insert into crawlerbatch (channel,phase,batchNum,crawlBegin,crawlEnd,day,getFlag)
          Values('$channel','$phase',$batchNum,'$crawlBegin','$crawlEnd',$date,true)
      """)
      .toString()
  }

  def genWorkCntQuery(channel: String, phase: String, batchNum: Int, date: String): String = {
    new StringBuilder().append(
      s"""
         |select count(*) as workcnt from crawler_jobhistory where
         |channel='$channel'
         |and phase='$phase'
         |and begintime like '%$date%'
         |and dailyserial=$batchNum
         |and datapath is not null
       """.stripMargin
    ).toString()
  }

  def genCntQuery(channel: String, phase: String, batchNum: Int, date: String): String = {
    new StringBuilder().append("select count(*) as cnt from crawler_jobhistory where ")
      .append(" channel='" + channel + "'")
      .append(" and phase='" + phase + "'")
      .append(" and dailyserial=" + batchNum)
      .append(" and begintime like '%" + date + "%'").toString()
  }

  def genAppgoCrawlerQuery(channel: String, phase: String, batchNum: Int, date: String): String = {
    new StringBuilder().append("select worknode,datapath,begintime,finishtime from crawler_jobhistory where ")
      .append(" channel='" + channel + "'")
      .append(" and phase='" + phase + "'")
      .append(" and dailyserial=" + batchNum)
      .append(" and begintime like '%" + date + "%'").toString()
  }

  /**
   * 检查本地文件是否存在
   *
   * @param workNode
   * @param dataPath
   * @param batchNum
   * @param date
   * @return
   */
  def checkLocalFileExists(workNode: String, dataPath: String, batchNum: Int, date: String): Boolean = {
    val dataPathArr = dataPath.split("/")
    val fileName = dataPathArr.apply(7)
    val localFilePath = makeLocalFilePath(workNode, dataPath, batchNum, date)
    val file: File = new File(localFilePath + fileName)
    file.exists()
  }

  /**
   * 检查HDFS文件是否存在
   *
   * @param workNode
   * @param dataPath
   * @param batchNum
   * @param date
   * @return
   */
  def checkHdfsFileExists(workNode: String, dataPath: String, batchNum: Int, date: String): Boolean = {
    val dataPathArr = dataPath.split("/")
    val fileName = dataPathArr.apply(7)
    val hdfsFilePath = makeHdfsFilePath(workNode, dataPath, batchNum, date)
    HDFSUtils.isExists(hdfsFilePath + fileName)
  }

  /**
   * /data/Appgo_Crawler/10.5.33.100/Appgo_app/crawler-producer/data/googleplayCategoryChart/list_page_phase/
   * /data/logs/appgo_crawler/data/itunesCategoryChart/detail_page_phase/20161217-000000.tar.gz
   * /data/Appgo_Crawler/10.5.33.100/logs/appgo_crawler/data/googleplayCategoryChart/detail_page_phase
   * 生成远程源文件存放路径
   * 带文件名
   *
   * @param workNode
   * @param dataPath
   * @param batchNum
   * @return
   */
  def makeRemoteFilePath(workNode: String, dataPath: String, batchNum: Int): String = {
    val dataPathArr = dataPath.split("/")
    val channel = dataPathArr.apply(5)
    val phase = dataPathArr.apply(6)
    val ip = workNode2IP(workNode)
    val fileName = dataPathArr.apply(7)
    val remoteLocalHomePath = "/data/Appgo_Crawler/"
    val remoteLocalFullPath = remoteLocalHomePath +
      ip + "/logs/appgo_crawler/data/" + channel + "/" + phase + "/" + fileName
    remoteLocalFullPath
  }

  /**
   * 生成集群客户端本地文件存放路径，带文件名
   *
   * @param workNode
   * @param dataPath
   * @param batchNum
   * @param date
   * @return
   */
  def makeLocalDestFile(workNode: String, dataPath: String, batchNum: Int, date: String): String = {
    val dataPathArr = dataPath.split("/")
    val fileName = dataPathArr.apply(7)
    val localDestFile = makeLocalFilePath(workNode, dataPath, batchNum, date) + fileName
    localDestFile
  }

  /**
   * /data/crawlerData/rawData/googleplayCategoryChart/20161113/
   * list_page_phase/10.5.33.94/crawler-producer-1/20161113-010008
   * 生成集群客户端本地文件存放路径，不带文件名
   *
   * @param workNode
   * @param dataPath
   * @param batchNum
   * @param date
   * @return
   */
  def makeLocalFilePath(workNode: String, dataPath: String, batchNum: Int, date: String): String = {
    val dataPathArr = dataPath.split("/")
    val channel = dataPathArr.apply(5)
    val phase = dataPathArr.apply(6)
    val ip = workNode2IP(workNode)
    val crawlerProducer = dataPathArr.apply(3)
    val localHomePath = "/data/crawlerData/rawData/"
    val localFullPath = localHomePath + channel + "/" + date + "/" + phase +
      "/" + ip + "/" + crawlerProducer + "/" + batchNum + "/"
    localFullPath
  }

  /**
   * /user/dba/crawlerData/googleplayCategoryChart/20161113/list_page_phase/10.5.33.94/crawler-producer-1/1/
   * 生成HDFS路径不带文件名
   *
   * @param workNode
   * @param dataPath
   * @return
   */
  def makeHdfsFilePath(workNode: String, dataPath: String, batchNum: Int, date: String): String = {
    val dataPathArr = dataPath.split("/")
    val channel = dataPathArr.apply(5)
    val phase = dataPathArr.apply(6)
    val ip = workNode2IP(workNode)
    val crawlerProducer = dataPathArr.apply(3)
    val hdfsHomePath = "/user/dba/crawlerData/"
    val hdfsFullPath = hdfsHomePath + channel + "/" + date + "/" +
      phase + "/" + ip + "/" + crawlerProducer + "/" + batchNum + "/"
    hdfsFullPath
  }

  /**
   * workNode与Ip转换
   *
   * @param workNode
   * @return
   */
  def workNode2IP(workNode: String): String = {
    val workNode2IpMap: mutable.HashMap[String, String] = mutable.HashMap(
      ("Appgo_app_33_111", "10.5.33.111"),
      ("Appgo_app_33_110", "10.5.33.110"),
      ("Appgo_app_33_109", "10.5.33.109"),
      ("Appgo_app_33_108", "10.5.33.108"),
      ("Appgo_app_33_100", "10.5.33.100"),
      ("Appgo_app_33_101", "10.5.33.101"),
      ("Appgo_app_33_102", "10.5.33.102"),
      ("Appgo_app_33_103", "10.5.33.103"),

      ("Appgo_app_Crawler_33_50", "10.5.33.50"),
      ("Appgo_app_Crawler_33_51", "10.5.33.51"),
      ("Appgo_app_Crawler_33_52", "10.5.33.52"),
      ("Appgo_app_Crawler_33_53", "10.5.33.53"),
      ("Appgo_app_Crawler_33_54", "10.5.33.54"),
      ("Appgo_app_Crawler_33_55", "10.5.33.55"),
      ("Appgo_app_Crawler_33_56", "10.5.33.56"),
      ("Appgo_app_Crawler_33_57", "10.5.33.57"),
      ("Appgo_app_Crawler_33_58", "10.5.33.58"),
      ("Appgo_app_Crawler_33_59", "10.5.33.59"),

      ("Appgo_app_Crawler_33_60", "10.5.33.60"),
      ("Appgo_app_Crawler_33_61", "10.5.33.61"),
      ("Appgo_app_Crawler_33_62", "10.5.33.62"),
      ("Appgo_app_Crawler_33_63", "10.5.33.63"),
      ("Appgo_app_Crawler_33_64", "10.5.33.64"),
      ("Appgo_app_Crawler_33_65", "10.5.33.65"),
      ("Appgo_app_Crawler_33_66", "10.5.33.66"),
      ("Appgo_app_Crawler_33_67", "10.5.33.67"),
      ("Appgo_app_Crawler_33_68", "10.5.33.68"),
      ("Appgo_app_Crawler_33_69", "10.5.33.69"))
    workNode2IpMap.get(workNode).getOrElse("").toString
  }
}
