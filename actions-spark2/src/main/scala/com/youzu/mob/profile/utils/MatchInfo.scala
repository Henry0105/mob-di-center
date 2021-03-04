package com.youzu.mob.profile.utils

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

class OutCnt {
  val m = mutable.Map.empty[String, Long]

  def set(k: String, v: Long): Unit = {
    m.update(k, v)
  }

  def toJson(): JValue = {
    render(map2jvalue(m.toMap[String, Long]))
  }

  def toJsonString(): String = {
    compact(toJson())
  }
}

case class MatchInfo(jobId: String, uuid: String, idCnt: Long, matchCnt: Long, outCnt: OutCnt) {
  val m = mutable.Map.empty[String, Long]

  def set(k: String, v: Long): Unit = {
    m.update(k, v)
  }

  def toJsonString(): String = {
    val json =
      ("job_id" -> jobId) ~ ("uuid" -> uuid) ~ ("id_cnt" -> idCnt) ~
        ("match_cnt" -> matchCnt) ~ ("out_cnt" -> outCnt.toJson())
    compact(render(json).merge(render(map2jvalue(m.toMap[String, Long]))))
  }
}
