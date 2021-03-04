package com.youzu.mob.compress.util

import com.youzu.mob.compress.Point

import scala.collection.Iterator

class DouglasPeucker {
  var index_list: List[Int] = List()

  def height_calc(p1: Point, p2: Point, point: Point): Double = {
    var area = Math.abs(0.5 * (p1.lat * p2.lon + p2.lat * point.lon + point.lat * p1.lon - p2.lat * p1.lon
      - point.lat * p2.lon - p1.lat * point.lon))
    var bottom = math.sqrt(math.pow(p1.lat - p2.lat, 2) + math.pow(p1.lon - p2.lon, 2))
    var height = 2 * area / bottom
    height
  }

  def douglaspeucker(pointlist: Array[Point], firstpoint: Int, lastpoint: Int, tolerance: Double): List[Int] = {
    var maxdistance = 0.0
    var index = 0
    if (pointlist == null || pointlist.size <= 2) {
      for (i <- (0 to pointlist.length - 1)) {
        index_list = index_list :+ i
      }
      return index_list
    } else {
      var p1 = pointlist(firstpoint)
      var p2 = pointlist(lastpoint)
      var distance = 0.0
      for (i <- (firstpoint to lastpoint)) {
        var p = pointlist(i)
        var dot = (p2.lat - p1.lat) * (p.lat - p1.lat) + (p2.lon - p1.lon) * (p.lon - p1.lon)
        var d2 = (p2.lat - p1.lat) * (p2.lat - p1.lat) + (p2.lon - p1.lon) * (p2.lon - p1.lon)
        if (dot <= 0) {
          distance = math.sqrt(
            math.pow(p.lat - p1.lat, 2) + math.pow(p.lon - p1.lon, 2))
        } else if (dot >= d2) {
          distance = math.sqrt(
            math.pow(p.lat - p2.lat, 2) + math.pow(p.lon - p2.lon, 2))
        } else {
          distance = height_calc(p1, p2, p)
        }
        if (distance >= maxdistance) {
          maxdistance = distance
          index = i
        }
      }
      if (maxdistance >= tolerance && index != 0) {
        index_list = index_list :+ index
        douglaspeucker(pointlist, firstpoint, index, tolerance)
        douglaspeucker(pointlist, index, lastpoint, tolerance)
      }
      index_list
    }
  }

  def compress(pointlist: Array[Point], tolerance: Double): Array[Point] = {
    if (pointlist == null || pointlist.size <= 2) {
      return pointlist
    } else {
      var firstpoint = 0
      var lastpoint = pointlist.size - 1
      //      查看最后一个点跟第一个点是否为一个点
      while (pointlist(firstpoint).lat == pointlist(lastpoint).lat
        && pointlist(firstpoint).lon == pointlist(lastpoint).lon && lastpoint > 0) {
        lastpoint = lastpoint - 1
      }
      if (firstpoint == lastpoint) {
        index_list = index_list :+ firstpoint
      } else {
        index_list = index_list :+ firstpoint
        index_list = index_list :+ lastpoint
        douglaspeucker(pointlist, firstpoint, lastpoint, tolerance)
      }
        index_list.sortWith(_ > _)
        var final_list: Array[Point] = Array()
        final_list = (for (i <- index_list) yield {
          pointlist(i)
        }).toArray
        return final_list
      }
  }
}
