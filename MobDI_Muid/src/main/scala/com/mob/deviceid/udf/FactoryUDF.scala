package com.mob.deviceid.udf

import com.mob.deviceid.config.HiveProps

import scala.collection.mutable

class FactoryUDF {
  val factoryGroup: Map[String, String] = {
    val map = new mutable.HashMap[String, String]()
    HiveProps.HEAD_FACTORY.split(",").foreach(s => {
      if (s.contains(" ")) {
        val fgElement = s.split(" ")
        val factory = fgElement.head
        fgElement.foreach(map.put(_, factory))
      } else {
        map.put(s, s)
      }
    })
    map.toMap
  }

  def factory_classify(f: String): String = {
    if (f == null) {
      return "other"
    }
    factoryGroup.getOrElse(f, "other")
  }
}
