package com.mob.appgo.common.mysql

import scala.collection.mutable

trait DBUtilsInter {
  def execute(sql: String): Boolean

  def executeBatch(sql: String): Array[Int]

  def query(sql: String): scala.Array[mutable.HashMap[String, String]]

  def update(sql: String): Boolean

  def close()
}