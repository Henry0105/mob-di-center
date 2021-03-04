package com.youzu.mob.profile.tags_info_full.beans

case class Param(day: String = "") {

  override def toString: String =
    s"""
       |day=$day
       |""".stripMargin
}