package com.mob.appgo.common.mysql

class DBInfo {
  private var driver: String = null
  private var url: String = null
  private var userName: String = null
  private var passWord: String = null

  def getDriver: String = driver

  def setDriver(driver: String) {
    this.driver = driver
  }

  def getUrl: String = url

  def setUrl(url: String) {
    this.url = url
  }

  def getUserName: String = userName

  def setUserName(userName: String) {
    this.userName = userName
  }

  def getPwd: String = passWord

  def setPwd(passWord: String) {
    this.passWord = passWord
  }
}