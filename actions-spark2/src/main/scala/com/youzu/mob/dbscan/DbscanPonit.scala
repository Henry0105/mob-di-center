package com.youzu.mob.dbscan

import scala.collection.mutable.ArrayBuffer

class DbscanPonit {
  var lat = 0.0 // 纬度
  var lon = 0.0 // 经度
  var geohash6 = "" // 此point的六位geohash
  var geohashAdjacent = Array[String]() // 此经纬度邻近的8个geohash
  var centerLat: Double = 0.0 // 所在cluster的中心点纬度
  var centerLon: Double = 0.0 // 所在cluster的中心点经度

  var indexNum = 0 // 此Point在train时所在的数组下标
  var cluster = 0 // point分组类型
  var pointType = -1 // 用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
  var visited = 0 // 用于判断该点是否处理过，0表示未处理过
  var day = ""
  var hour = ""
  var weight = 1d

  // 求出point点在集合points中所有的邻域对象，ePs为邻域半径（km）
  def neighPoints(points: Array[DbscanPonit], ePs: Double): ArrayBuffer[DbscanPonit] = {
    val disPoints = points.filter(x => x.geohash6.equals(geohash6) ||
      geohashAdjacent.contains(x.geohash6)).map(x => (x, distanceGPS(x)))
    val arrayBuffer = ArrayBuffer[DbscanPonit]()
    arrayBuffer ++= disPoints.filter(x => x._2 <= ePs).map(_._1)
    arrayBuffer
  }

  // 求point点到other点的距离(gps)
  def distanceGPS(other: DbscanPonit): Double = {
    val pi = 3.1415926
    val r: Double = 6370.99681
    // a1、a2、b1、b2分别为上面数据的经纬度转换为弧度
    val a1 = other.lat * pi / 180.0
    val a2 = other.lon * pi / 180.0
    val b1 = lat * pi / 180.0
    val b2 = lon * pi / 180.0
    val t1: Double = Math.cos(a1) * Math.cos(a2) * Math.cos(b1) * Math.cos(b2)
    val t2: Double = Math.cos(a1) * Math.sin(a2) * Math.cos(b1) * Math.sin(b2)
    val t3: Double = Math.sin(a1) * Math.sin(b1)
    val distance = Math.acos(t1 + t2 + t3) * r
    distance

  }

}
