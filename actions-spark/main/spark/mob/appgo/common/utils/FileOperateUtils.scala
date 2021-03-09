package com.mob.appgo.common.utils

import java.io.{File, IOException, InputStream, OutputStream, _}

object FileOperateUtils {

  def createFile(destFileName: String): Boolean = {
    val file: File = new File(destFileName)
    if (file.exists) {
      System.out.println("创建单个文件" + destFileName + "失败，目标文件已存在！")
      return false
    }
    if (destFileName.endsWith(File.separator)) {
      System.out.println("创建单个文件" + destFileName + "失败，目标文件不能为目录！")
      return false
    }
    if (!file.getParentFile.exists) {
      System.out.println("目标文件所在目录不存在，准备创建它！")
      if (!file.getParentFile.mkdirs) {
        System.out.println("创建目标文件所在目录失败！")
        return false
      }
    }
    try {
      if (file.createNewFile) {
        System.out.println("创建单个文件" + destFileName + "成功！")
        true
      }
      else {
        System.out.println("创建单个文件" + destFileName + "失败！")
        false
      }
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
        System.out.println("创建单个文件" + destFileName + "失败！" + e.getMessage)
        false
    }
  }

  def createDirs(destDirName: String): Boolean = {
    val dir: File = new File(destDirName)
    var destDirNameFormat: String = destDirName
    if (dir.exists) {
      System.out.println("创建目录" + destDirNameFormat + "失败，目标目录已经存在")
      return false
    }
    if (!destDirNameFormat.endsWith(File.separator)) {
      destDirNameFormat = destDirNameFormat + File.separator
    }
    if (dir.mkdirs) {
      System.out.println("创建目录" + destDirNameFormat + "成功！")
      true
    }
    else {
      System.out.println("创建目录" + destDirNameFormat + "失败！")
      false
    }
  }

  def copyFile(srcFileName: String, destFileName: String, overwrite: Boolean): Boolean = {
    val srcFile: File = new File(srcFileName)
    if (!srcFile.exists) {
      System.out.println("源文件：" + srcFileName + "不存在！")
      return false
    }
    else if (!srcFile.isFile) {
      System.out.println("复制文件失败，源文件：" + srcFileName + "不是一个文件！")
      return false
    }
    val destFile: File = new File(destFileName)
    if (destFile.exists) {
      if (overwrite) {
        new File(destFileName).delete
      }
    }
    else {
      if (!destFile.getParentFile.exists) {
        if (!destFile.getParentFile.mkdirs) {
          return false
        }
      }
    }
    var byteread: Int = 0
    var in: InputStream = null
    var out: OutputStream = null
    try {
      in = new FileInputStream(srcFile)
      out = new FileOutputStream(destFile)
      val buffer: Array[Byte] = new Array[Byte](1024)
      while ( {
        byteread = in.read(buffer)
        byteread
      } != -1) {
        out.write(buffer, 0, byteread)
      }
      true
    }
    catch {
      case e: FileNotFoundException =>
        false
      case e: IOException =>
        false
    } finally {
      try {
        if (out != null) out.close()
        if (in != null) in.close()
      }
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
  }
}