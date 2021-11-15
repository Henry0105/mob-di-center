package com.youzu.mob.sink

import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.spark.sql.Row

import scala.collection.mutable

object BuildUtils {

  def buildRows(rows: Array[Row]): Seq[Array[String]] = {
    var prevField: String = ""
    var lines: mutable.ListBuffer[Array[String]] = mutable.ListBuffer.empty[Array[String]]
    lines.append(rows(0).schema.fieldNames)
    rows.foreach(r => {
      lines.append(r.toSeq.mkString("&&").split("&&").toArray[String])
    })

    lines
  }

  def buildExcel(lines: Seq[Array[String]], sheetName: String): SXSSFWorkbook = {
    val wb: SXSSFWorkbook = new SXSSFWorkbook()
    val sheet: Sheet = wb.createSheet(sheetName)
    lines.zipWithIndex.foreach {
      case (line, i) =>
        val row = sheet.createRow(i)
        line.zipWithIndex.foreach {
          case (item, j) =>
            val cell: Cell = row.createCell(j)
            cell.setCellValue(item)
        }
    }
    wb
  }

  def isNumberStr(str: String): Boolean = {
    str match {
      case x if x.isEmpty => false
      case x if x.startsWith("-") => x.substring(1).forall(_.isDigit)
      case x => x.forall(_.isDigit)
    }
  }

}
