package com.youzu.mob.mydbscan

import com.youzu.mob.geohash.GeoHash

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object DbscanTools {

  def setBoundaryPoint(points: Array[DbscanPonit], other: ArrayBuffer[DbscanPonit]): Unit = {
    breakable {
      for (i <- other) {
        if (points(i.indexNum).pointType == 1) {
          points(i.indexNum).pointType = 0
          break
        }
      }
    }
  }

  def setEveryPointRoughNeigh(points: Array[DbscanPonit], isGeohase: Boolean): Unit = {

    for (i <- 0 to points.length - 1) {
      points(i).indexNum = i
      if (isGeohase) {
        val geohash = GeoHash.geoHashStringWithCharacterPrecision(
          points(i).lat, points(i).lon, 6)
        points(i).geohash6 = geohash
        val geohashStr = GeoHash.fromGeohashString(geohash)
        points(i).geohashAdjacent = geohashStr.getAdjacent.map(_.toBase32)
      }
    }

  }

  def centerPoint(points: Array[DbscanPonit]): Unit = {
    val cMap = scala.collection.mutable.Map[Int, (Double, Double)]()
    points.map(p => (p.cluster, p.lat, p.lon))
      .groupBy(_._1).foreach(map => {
      var X = 0.toDouble
      var Y = 0.toDouble
      var Z = 0.toDouble
      val total = map._2.length
      for (item <- map._2) {
        val lat = item._2 * Math.PI / 180
        val lon = item._3 * Math.PI / 180
        var x = Math.cos(lat) * Math.cos(lon)
        var y = Math.cos(lat) * Math.sin(lon)
        var z = Math.sin(lat)
        X += x
        Y += y
        Z += z
      }
      X = X / total
      Y = Y / total
      Z = Z / total
      val Lon = Math.atan2(Y, X)
      val Hyp = Math.sqrt(X * X + Y * Y)
      val Lat = Math.atan2(Z, Hyp)

      cMap += (map._1 -> (Lat * 180 / Math.PI, Lon * 180 / Math.PI))
    })

    for (i <- 0 to points.length - 1) {
      points(i).centerLat = cMap.get(points(i).cluster).get._1
      points(i).centerLon = cMap.get(points(i).cluster).get._2
    }
  }
}
