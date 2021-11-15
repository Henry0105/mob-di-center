package com.youzu.mob.sortsystem

import net.sf.json.JSONObject

object DbInfo {

  def convertToDbInfo(mysqlInfoStr: String): DbInfo = {
    val dbInfo = new DbInfo
    val jsonObject = JSONObject.fromObject(mysqlInfoStr)

    dbInfo.userName = jsonObject.getString("userName")
    dbInfo.pwd = jsonObject.getString("pwd")
    dbInfo.DbName = jsonObject.getString("dbName")
    dbInfo.host = jsonObject.getString("host")
    dbInfo.tableName = jsonObject.getString("tableName")
    dbInfo.port = jsonObject.getInt("port")

    dbInfo
  }

  def getJDBCUrl(dbInfo: DbInfo): String = {
    val baseStr = "jdbc:mysql://%s:%s/%s?charset-utf8"
    baseStr.format(dbInfo.host, dbInfo.port.toString, dbInfo.DbName)

  }

  def getDbTable(dbInfo: DbInfo): String = {
    dbInfo.DbName + "." + dbInfo.tableName
  }

}

class DbInfo extends Serializable {
  var userName: String = _
  var pwd: String = _
  var DbName: String = _
  var tableName: String = _
  var port: Int = 3306
  var host: String = _

  override def toString: String = {
    userName + " " + pwd + " " + DbName + " " + tableName + " " + host + " " + port.toString
  }

}
