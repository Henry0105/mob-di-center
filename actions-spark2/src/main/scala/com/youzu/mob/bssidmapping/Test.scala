package youzu.mob.bssidmapping


object Test {
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

    println(getCenter(Seq(0.1, 0.2, 0.3), null)._1, getCenter(Seq(0.1, 0.2, 0.3), Seq(0.1, 0.2, 0.3))._2)

  }

}
