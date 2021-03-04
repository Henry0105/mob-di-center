package com.youzu.mob.tools

object GPSDistance {

  def getDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val pi = 3.1415926
    val r: Double = 6370.99681
    val a1 = lat1 * pi / 180.0
    val a2 = lon1 * pi / 180.0
    val b1 = lat2 * pi / 180.0
    val b2 = lon2 * pi / 180.0
    var t1: Double = Math.cos(a1) * Math.cos(a2) * Math.cos(b1) * Math.cos(b2)
    var t2: Double = Math.cos(a1) * Math.sin(a2) * Math.cos(b1) * Math.sin(b2)
    var t3: Double = Math.sin(a1) * Math.sin(b1)
    val distance = Math.acos(t1 + t2 + t3) * r
    distance
  }

  def getCenter(latlist: Seq[Double], lonlist: Seq[Double]): (Double, Double) = {
    if (latlist.size == 0 || lonlist.size == 0) {
      throw new NullPointerException
    }
    val geoCoordinateList = latlist zip lonlist
    val total = geoCoordinateList.length

    var X: Double = 0
    var Y: Double = 0
    var Z: Double = 0
    for (geoCoordinate <- geoCoordinateList) {
      var lat: Double = 0
      var lon: Double = 0
      var x: Double = 0
      var y: Double = 0
      var z: Double = 0
      lat = geoCoordinate._1 * Math.PI / 180
      lon = geoCoordinate._2 * Math.PI / 180
      x = Math.cos(lat) * Math.cos(lon)
      y = Math.cos(lat) * Math.sin(lon)
      z = Math.sin(lat)
      X += x
      Y += y
      Z += z
    }
    X = X / total
    Y = Y / total
    Z = Z / total
    val Lon: Double = Math.atan2(Y, X)
    val Hyp: Double = Math.sqrt(X * X + Y * Y)
    val Lat: Double = Math.atan2(Z, Hyp)
    val latCenter = Lat * 180 / Math.PI
    val lonCenter = Lon * 180 / Math.PI
    (latCenter, lonCenter)
  }

  def main(args: Array[String]): Unit = {

    println(getDistance(56.45, 12.23, 56.43, 12.34))

  }

}

