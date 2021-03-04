package com.youzu.mob.mydbscan

import scala.collection.mutable.ArrayBuffer

object Dbscan {
  def train(points: Array[DbscanPonit], ePs: Double,
            minPoints: Int, isGeohase: Boolean): Unit = {
    try {
      DbscanTools.setEveryPointRoughNeigh(points, isGeohase)
      new Dbscan(points, ePs, minPoints).train(points)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}

class Dbscan private(points: Array[DbscanPonit], ePs: Double,
                     minPoints: Int) extends Serializable {
  var number = 1

  def train(points: Array[DbscanPonit]): Unit = {
    var neighPoints = ArrayBuffer[DbscanPonit]()
    var neighPointsTmp = ArrayBuffer[DbscanPonit]()
    var index = -1
    for {
      point <- points
      if point.visited == 0
    } yield {
      neighPoints = point.neighPoints(points, ePs)
      //      if (neighPoints.length > 1 && neighPoints.length < minPoints) {
      //        //此为非核心点，若其领域内有核心点，则该点为边界点
      //        //DbscanTools.setBoundaryPoint(points, neighPoints)
      //      }
      if (neighPoints.length >= minPoints) {
        points(point.indexNum).pointType = 1
        points(point.indexNum).cluster = number

        while (!neighPoints.isEmpty) {
          index = neighPoints.head.indexNum
          if (points(index).visited == 0) {
            points(index).visited = 1
            if (points(index).cluster == 0) points(index).cluster = number
            neighPointsTmp = points(index).neighPoints(points, ePs)

            if (neighPointsTmp.length >= minPoints) {
              points(index).pointType = 1
              for (p <- neighPointsTmp) {
                if (points(p.indexNum).cluster == 0) {
                  points(p.indexNum).cluster = number
                  neighPoints += p
                }
              }
            }

          }
          neighPoints.remove(0)
        }
        number += 1
      }
    }
  }
}
