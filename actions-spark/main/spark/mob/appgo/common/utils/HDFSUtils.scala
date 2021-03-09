package com.mob.appgo.common.utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.DatanodeInfo

object HDFSUtils {

  def getConf: Configuration = {
    val conf: Configuration = new Configuration
    conf
  }


  @throws(classOf[IOException])
  def getDateNodeHost(): Array[String] = {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val hdfs: DistributedFileSystem = fs.asInstanceOf[DistributedFileSystem]
    val dataNodeStats: Array[DatanodeInfo] = hdfs.getDataNodeStats
    dataNodeStats.map(dataNodeInfo => dataNodeInfo.getHostName)
  }

  @throws(classOf[IOException])
  def isExists(fullPath: String): Boolean = {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val path: Path = new Path(fullPath)
    fs.exists(path)
  }

  @throws(classOf[IOException])
  def mkdir(dir: String) {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    fs.mkdirs(new Path(dir))
    fs.close()
  }

  @throws(classOf[IOException])
  def deleteDir(dir: String) {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    fs.delete(new Path(dir), true)
    fs.close()
  }


  @throws(classOf[IOException])
  def uploadLocalFile2HDFS(localFileFullPath: String, destHDFSFullPath: String) {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val src: Path = new Path(localFileFullPath)
    val dst: Path = new Path(destHDFSFullPath)
    fs.copyFromLocalFile(src, dst)
    fs.close()
  }

  @throws(classOf[IOException])
  def createNewHDFSFile(toCreateFilePath: String, content: String) {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val os: FSDataOutputStream = fs.create(new Path(toCreateFilePath))
    os.write(content.getBytes("UTF-8"))
    os.close()
    fs.close()
  }


  @throws(classOf[IOException])
  def deleteHDFSFile(dst: String): Boolean = {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val path: Path = new Path(dst)
    val isDeleted: Boolean = fs.delete(path, true)
    fs.close()
    isDeleted
  }

  @throws(classOf[Exception])
  def readHDFSFile(dst: String): Array[Byte] = {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val path: Path = new Path(dst)
    if (fs.exists(path)) {
      val is: FSDataInputStream = fs.open(path)
      val stat: FileStatus = fs.getFileStatus(path)
      val buffer: Array[Byte] = new Array[Byte](String.valueOf(stat.getLen).toInt)
      is.readFully(0, buffer)
      is.close()
      fs.close()
      buffer
    }
    else {
      throw new Exception("the file is not found .")
    }
  }

  @throws(classOf[IOException])
  def listAll(dir: String) {
    val conf: Configuration = getConf
    val fs: FileSystem = FileSystem.get(conf)
    val stats: Array[FileStatus] = fs.listStatus(new Path(dir))
    stats.map(stat => {
      if (!stat.isDirectory) stat.getPath.toString
      else stat.getPath.toString
    })
    fs.close()
  }
}