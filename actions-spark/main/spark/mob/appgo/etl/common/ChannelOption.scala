package com.mob.appgo.etl.common


object ChannelOption extends Enumeration {
  type ChnnelOption = Value
  val GoogleCATE = Value("googleplayCategoryChart")
  val GoogleFEAT = Value("googleplayFeatureChart")
  val GoogleSEAR = Value("googleplaySearchChart")
  val ItunesCATE = Value("itunesCategoryChart")
  val ItunesFEAT = Value("itunesFeatureChart")
  val ItunesSEAR = Value("itunesSearchChart")
  val QQCATE = Value("qqCategoryChart")
}
