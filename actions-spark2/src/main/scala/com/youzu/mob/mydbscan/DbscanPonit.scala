package com.youzu.mob.mydbscan

import scala.collection.mutable.ArrayBuffer

class DbscanPonit {
  var lat = 0.0
  var lon = 0.0
  var geohash6 = ""
  var geohashAdjacent = Array[String]()
  var centerLat = 0.0
  var centerLon = 0.0

  var indexNum = 0
  var cluster = 0
  var pointType = -1
  var visited = 0
  def neighPoints(points: Array[DbscanPonit], ePs: Double): ArrayBuffer[DbscanPonit] = {
    val disPoints = points.
      filter(x => x.geohash6.equals(geohash6) || geohashAdjacent.contains(x.geohash6)).
      map(x => (x, distanceGPS(x)))
    val arrayBuffer = ArrayBuffer[DbscanPonit]()
    arrayBuffer ++= disPoints.filter(x => x._2 <= ePs).map(_._1)
    arrayBuffer
  }

  def distanceGPS(other: DbscanPonit): Double = {
    val pi = 3.1415926
    val r: Double = 6370.99681
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

  override def toString: String =
    s"""
       |lat=${lat} ,lon=${lon} ,cluster=${cluster} , visited=${visited} , indexNum=${indexNum}
    """.stripMargin

}
