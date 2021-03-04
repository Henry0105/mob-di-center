package com.youzu.mob.profile.tags_info_full.beans

case class TagsCheckerParam(
                                   day: String = "",
                                   test: Boolean = false,
                                   exclude: String = "") {

  override def toString: String =
    s"""
       |day=$day, test=$test, exclude=$exclude
       |""".stripMargin
}