package com.youzu.mob.dbscan

import scala.collection.mutable.ArrayBuffer

object Dbscan {
  def train(points: Array[DbscanPonit], ePs: Double,
            minPoints: Int, isGeohase: Boolean, trainType: Int): Unit = {
    try {
      DbscanTools.setEveryPointRoughNeigh(points, isGeohase)

      if (trainType == 1) {
        new Dbscan(points, ePs, minPoints).train(points)
      } else if (trainType == 2) {
        new Dbscan(points, ePs, minPoints).trainWithEps(points)
      } else if (trainType == 3) {
        new Dbscan(points, ePs, minPoints).trainWithMinPoints(points)
      } else {
        new Dbscan(points, ePs, minPoints).trainWithPoints(points)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}

class Dbscan private(points: Array[DbscanPonit], ePs: Double,
                     minPoints: Int) extends Serializable {
  var number = 1 //用于标记类

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
        // 核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
        points(point.indexNum).pointType = 1
        points(point.indexNum).cluster = number

        while (!neighPoints.isEmpty) {
          index = neighPoints.head.indexNum
          if (points(index).visited == 0) {
            // 若该点未被处理，则标记已处理
            points(index).visited = 1
            if (points(index).cluster == 0) points(index).cluster = number
            neighPointsTmp = points(index).neighPoints(points, ePs)

            if (neighPointsTmp.length >= minPoints) {
              // 将其领域内未分类的对象划分到簇中,然后放入neighPoints
              points(index).pointType = 1
              for (p <- neighPointsTmp) {
                if (points(p.indexNum).cluster == 0) {
                  points(p.indexNum).cluster = number // 只划分簇，没有访问到
                  neighPoints += p
                }
              }
            }
            //            if (neighPointsTmp.length > 1 && neighPointsTmp.length < minPoints) {
            //              //此为非核心点，若其领域内有核心点，则该点为边界点
            //             // DbscanTools.setBoundaryPoint(points, neighPointsTmp)
            //            }

          }
          neighPoints.remove(0)
        }
        number += 1 // 进行新的聚类
      }
    }
  }


  def trainWithEps(points: Array[DbscanPonit]): Unit = {
    var neighPoints = ArrayBuffer[DbscanPonit]()
    var neighPointsTmp = ArrayBuffer[DbscanPonit]()
    var index = -1
    for {
      point <- points
      if point.visited == 0
    } yield {
      neighPoints = point.neighPoints(points, point.weight * ePs)
      //      if (neighPoints.length > 1 && neighPoints.length < minPoints) {
      //        //此为非核心点，若其领域内有核心点，则该点为边界点
      //        //DbscanTools.setBoundaryPoint(points, neighPoints)
      //      }

      if (neighPoints.length >= minPoints) {
        // 核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
        points(point.indexNum).pointType = 1
        points(point.indexNum).cluster = number

        while (!neighPoints.isEmpty) {
          index = neighPoints.head.indexNum
          if (points(index).visited == 0) {
            // 若该点未被处理，则标记已处理
            points(index).visited = 1
            if (points(index).cluster == 0) points(index).cluster = number
            neighPointsTmp = points(index).neighPoints(points, ePs)

            if (neighPointsTmp.length >= minPoints) {
              // 将其领域内未分类的对象划分到簇中,然后放入neighPoints
              points(index).pointType = 1
              for (p <- neighPointsTmp) {
                if (points(p.indexNum).cluster == 0) {
                  points(p.indexNum).cluster = number // 只划分簇，没有访问到
                  neighPoints += p
                }
              }
            }
            //            if (neighPointsTmp.length > 1 && neighPointsTmp.length < minPoints) {
            //              //此为非核心点，若其领域内有核心点，则该点为边界点
            //             // DbscanTools.setBoundaryPoint(points, neighPointsTmp)
            //            }

          }
          neighPoints.remove(0)
        }
        number += 1 // 进行新的聚类
      }
    }
  }


  def trainWithMinPoints(points: Array[DbscanPonit]): Unit = {
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

      if (neighPoints.length >= minPoints / point.weight) {
        // 核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
        points(point.indexNum).pointType = 1
        points(point.indexNum).cluster = number

        while (!neighPoints.isEmpty) {
          index = neighPoints.head.indexNum
          if (points(index).visited == 0) {
            // 若该点未被处理，则标记已处理
            points(index).visited = 1
            if (points(index).cluster == 0) points(index).cluster = number
            neighPointsTmp = points(index).neighPoints(points, ePs)

            if (neighPointsTmp.length >= minPoints) {
              // 将其领域内未分类的对象划分到簇中,然后放入neighPoints
              points(index).pointType = 1
              for (p <- neighPointsTmp) {
                if (points(p.indexNum).cluster == 0) {
                  points(p.indexNum).cluster = number // 只划分簇，没有访问到
                  neighPoints += p
                }
              }
            }
            //            if (neighPointsTmp.length > 1 && neighPointsTmp.length < minPoints) {
            //              //此为非核心点，若其领域内有核心点，则该点为边界点
            //             // DbscanTools.setBoundaryPoint(points, neighPointsTmp)
            //            }

          }
          neighPoints.remove(0)
        }
        number += 1 // 进行新的聚类
      }
    }
  }

  def trainWithPoints(points: Array[DbscanPonit]): Unit = {
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
      var neiLengthWithWeight = 0d
      for (neightPoint <- neighPoints) {
        neiLengthWithWeight = neiLengthWithWeight + neightPoint.weight
      }

      if (neiLengthWithWeight >= minPoints) {
        // 核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
        points(point.indexNum).pointType = 1
        points(point.indexNum).cluster = number

        while (!neighPoints.isEmpty) {
          index = neighPoints.head.indexNum
          if (points(index).visited == 0) {
            // 若该点未被处理，则标记已处理
            points(index).visited = 1
            if (points(index).cluster == 0) points(index).cluster = number
            neighPointsTmp = points(index).neighPoints(points, ePs)

            if (neighPointsTmp.length >= minPoints) {
              // 将其领域内未分类的对象划分到簇中,然后放入neighPoints
              points(index).pointType = 1
              for (p <- neighPointsTmp) {
                if (points(p.indexNum).cluster == 0) {
                  points(p.indexNum).cluster = number // 只划分簇，没有访问到
                  neighPoints += p
                }
              }
            }
            //            if (neighPointsTmp.length > 1 && neighPointsTmp.length < minPoints) {
            //              //此为非核心点，若其领域内有核心点，则该点为边界点
            //             // DbscanTools.setBoundaryPoint(points, neighPointsTmp)
            //            }

          }
          neighPoints.remove(0)
        }
        number += 1 // 进行新的聚类
      }
    }
  }
}
