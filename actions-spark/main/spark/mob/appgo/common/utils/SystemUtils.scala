package com.mob.appgo.common.utils

import java.io.{IOException, InputStreamReader, LineNumberReader}

object SystemUtils {
  def getOsType: String = {
    val osName: String = System.getProperty("os.name").toLowerCase()
    val osType: String = if (osName.contains("windows")) "windows"
    else if (osName.contains("linux")) "linux"
    else "null"
    osType
  }

  def getHostName: String = {
    if (getOsType.contains("windows")) System.getenv("COMPUTERNAME")
    else if (getOsType.contains("linux")) System.getenv("HOSTNAME")
    else "null"
  }

  def runCommand(cmd: Array[String], tp: Int): String = {
    val buf: StringBuffer = new StringBuffer(1000)
    var rt: String = "-1"
    try {
      val pos: Process = Runtime.getRuntime.exec(cmd)
      pos.waitFor
      if (tp == 1) {
        if (pos.exitValue == 0) {
          rt = "1"
        }
      }
      else {
        val ir: InputStreamReader = new InputStreamReader(pos.getInputStream)
        val input: LineNumberReader = new LineNumberReader(ir)
        var ln: String = ""
        while ( {
          ln = input.readLine
          ln
        } != null) {
          buf.append(ln + "<br>")
        }
        rt = buf.toString
        input.close()
        ir.close()
      }
    }
    catch {
      case e: IOException =>
        rt = e.toString
      case e: Exception =>
        rt = e.toString
    }
    rt
  }

  def runCommand(cmd: String, tp: Int): String = {
    val buf: StringBuffer = new StringBuffer(1000)
    var rt: String = "-1"
    try {
      val pos: Process = Runtime.getRuntime.exec(cmd)
      pos.waitFor
      if (tp == 1) {
        if (pos.exitValue == 0) {
          rt = "1"
        }
      }
      else {
        val ir: InputStreamReader = new InputStreamReader(pos.getInputStream)
        val input: LineNumberReader = new LineNumberReader(ir)
        var ln: String = ""
        while ( {
          ln = input.readLine
          ln
        } != null) {
          buf.append(ln + "<br>")
        }
        rt = buf.toString
        input.close()
        ir.close()
      }
    }
    catch {
      case e: IOException =>
        rt = e.toString
      case e: Exception =>
        rt = e.toString
    }
    rt
  }
}