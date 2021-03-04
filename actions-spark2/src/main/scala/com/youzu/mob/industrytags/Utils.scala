package com.youzu.mob.industrytags

import sun.misc.{BASE64Decoder, BASE64Encoder}

object Utils {


  def seq2Str(seq: Seq[String]): String = {
    if (seq.isEmpty) {
      return ""
    }
    val separator = ","
    val sb = StringBuilder.newBuilder
    for (s <- seq.sorted) {
      sb.append(s).append(separator)
    }
    sb.substring(0, sb.length - 1)
  }

  def seq2MapStr(seq: Seq[String]): String = {
    if (seq.isEmpty) {
      return "'',''"
    }
    val separator = ","
    val sb = StringBuilder.newBuilder
    for (s <- seq) {
      if (s.startsWith("__")) {
        sb.append("'").append(s.substring(2)).
          append("'").append(separator).append(" CAST(").append(s).append(" AS STRING) ").append(separator)
      } else {
        sb.append("'").append(s).append("'").
          append(separator).append(" CAST(").append(s).append(" AS STRING) ").append(separator)
      }
    }
    sb.substring(0, sb.length - 1)
  }

  // (based_field, "gender,agebin,city")   =>   based_field.gender,based_field.agebin,based_field.city
  def getMapKeyValueStr(field: String, mapKeys: String): String = {
    if (mapKeys == null || mapKeys.isEmpty) {
      return ""
    }
    val separator = ","
    val sb = StringBuilder.newBuilder
    for (key <- mapKeys.split(",")) {
      sb.append(field).append("['").append(key).append("'] AS ").append(key).append(separator)
    }
    sb.substring(0, sb.length - 1)
  }

  def seq2CaseWhenStr(seq: Seq[(String, String)]): String = {
    if (seq == null || seq.isEmpty) {
      return ""
    }
    val separator = ","
    val sb = StringBuilder.newBuilder
    for (s <- seq) {
      sb.append(s._2).append(" AS ").append(s._1).append(separator)
    }
    sb.toString()
  }

  def map2CaseWhenStr(map: Map[String, String]): String = {
    if (map == null || map.isEmpty) {
      return ""
    }
    val separator = ",\n"
    val sb = StringBuilder.newBuilder
    for (entry <- map) {
      sb.append(separator).append(entry._2).append(" AS ").append(entry._1)
    }
    sb.toString()
  }

  def map2Base64Str(map: Map[String, String]): String = {
    if (map == null || map.isEmpty) {
      return ""
    }
    val base64encoder = new BASE64Encoder
    val sb = StringBuilder.newBuilder
    for (entry <- map) {
      sb.append(entry._1).append(":").append(entry._2).append(";")
    }
    base64encoder.encode(sb.substring(0, sb.length - 1).getBytes).replaceAll("\\n|\\r", "")
  }

  def base64Str2Map(str: String): Map[String, String] = {
    var conditionRules: Map[String, String] = Map()
    if (str == null || str == "") {
      return conditionRules
    } else {
      val base64decoder = new BASE64Decoder
      new String(base64decoder.decodeBuffer(str)).split(";").foreach(
        rule => conditionRules += (rule.split(":")(0) -> rule.split(":")(1))
      )
    }
    conditionRules
  }

}
