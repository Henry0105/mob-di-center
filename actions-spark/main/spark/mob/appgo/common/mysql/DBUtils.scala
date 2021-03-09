package com.mob.appgo.common.mysql

import java.io.{BufferedInputStream, InputStream}
import java.sql._
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DBUtils(dbInfo: DBInfo) extends DBUtilsInter {
  private var conn: Connection = null
  private var stmt: Statement = null
  private[common] var prop: Properties = new Properties


  try {
    val in: InputStream = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader.getResourceAsStream("jdbc.properties")
    )
    prop.load(in)
    val jdbcDriverClassName = prop.getProperty(dbInfo.getDriver.toString)
    val jdbcUrl = prop.getProperty(dbInfo.getUrl.toString)
    val jdbcUserName = prop.getProperty(dbInfo.getUserName.toString)
    val jdbcPassword = prop.getProperty(dbInfo.getPwd.toString)
    in.close()
    conn = DriverManager.getConnection(jdbcUrl, jdbcUserName, jdbcPassword)
    stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  }
  catch {
    case e: Exception =>
      e.printStackTrace()
  }

  override def execute(sql: String): Boolean = {
    stmt.execute(sql)
  }

  override def executeBatch(sql: String): scala.Array[Int] = {
    val sqlArr: scala.Array[String] = sql.split(";")
    //    val ps=conn.prepareStatement(sql)
    for (i <- sqlArr.indices) stmt.addBatch(sqlArr(i))
    stmt.executeBatch()
  }

  override def query(sql: String): scala.Array[mutable.HashMap[String, String]] = {
    val reset: ResultSet = stmt.executeQuery(sql)
    val rsmd = reset.getMetaData
    val size = rsmd.getColumnCount
    val buffer = new ArrayBuffer[mutable.HashMap[String, String]]()
    while (reset.next()) {
      val map = mutable.HashMap[String, String]()
      for (i <- 1 to size) {
        map += (rsmd.getColumnLabel(i) -> reset.getString(i))
      }
      buffer += map
    }
    buffer.toArray
  }

  override def update(sql: String): Boolean = {
    stmt.executeUpdate(sql) > 0
  }

  override def close {
    try {
      stmt.close()
      conn.close()
    }
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }
}