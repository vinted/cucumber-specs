package com.vinted.cucumber.spark.ext

import org.apache.spark.sql.Row

object RowExt {
  implicit class HandyRow(row: Row) {
    def values = {
      row.toSeq.map {
        case (string: String) if string.isEmpty => """"""""
        case everythingElse => everythingElse
      }
    }
  }
}
