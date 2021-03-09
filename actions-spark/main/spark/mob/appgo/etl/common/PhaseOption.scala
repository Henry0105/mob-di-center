package com.mob.appgo.etl.common

object PhaseOption extends Enumeration {
  type PhaseOption = Value
  val DETAIL = Value("detail_page_phase")
  val LIST = Value("list_page_phase")
  val REVIEW = Value("review_page_phase")
}
