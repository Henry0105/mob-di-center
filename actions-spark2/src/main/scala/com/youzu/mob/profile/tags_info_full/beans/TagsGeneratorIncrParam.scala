package com.youzu.mob.profile.tags_info_full.beans

case class TagsGeneratorIncrParam(
                                   day: String = "",
                                   sample: Boolean = false,
                                   batch: Int = 10,
                                   test: Boolean = false,
                                   full: Boolean = false,
                                   exclude: String = "") {

  override def toString: String =
    s"""
       |day=$day, sample=$sample, batch=$batch, test=$test, full=$full, exclude=$exclude
       |""".stripMargin
}