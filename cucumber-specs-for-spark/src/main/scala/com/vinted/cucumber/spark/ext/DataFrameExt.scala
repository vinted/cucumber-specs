package com.vinted.cucumber.spark.ext

import com.vinted.cucumber.spark.ext.RowExt._
import com.vinted.cucumber.util.AsciiTable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.functions.{col, floor}
import org.scalactic.Prettifier
import scala.collection.JavaConversions._

object DataFrameExt {
  implicit class HandyDataFrame(df: DataFrame) {
    def reducePrecision(precision: Int = 3) = {
      val floatTypes = Seq(DoubleType, FloatType)
      val floatNames = df.schema.fields
        .filter { name => floatTypes.contains(name.dataType) }
        .map(_.name)
      floatNames.foldLeft(df) { (df, name) =>
        df.withColumn(name, floor(df(name) * 1000) / 1000.0)
      }
    }

    // TODO: refactor me!
    // Try using diff UDFs for each value by simply iterating over the DataFrame
    def diff(other: DataFrame) = {
      val subset = df.select(other.columns.map(col): _*)
      val rows = subset.collect()
      val otherRows = other.collect()

      if (rows.isEmpty && otherRows.isEmpty) {
        "*Both tables are empty*"
      } else if (rows.isEmpty) {
        val diffs = otherRows.map { row =>
          diffRows(missingRow(row.size), row)
        }

        AsciiTable(subset.columns, diffs)
      } else {
        val diffs = rows.map { expectedRow =>
          diffRows(
            expectedRow,
            closestMatch(otherRows, expectedRow).getOrElse(missingRow(expectedRow.size))
          )
        }

        AsciiTable(subset.columns, diffs)
      }
    }

    def toAscii(numRows: Integer = 10) = {
      val rows = df.limit(numRows).collect.map { row =>
        row.toSeq.map(Prettifier.default(_))
      }

      if (rows.isEmpty) {
        "*Empty table*"
      } else {
        AsciiTable(df.columns, rows)
      }
    }

    private

    def diffRows(expected: Row, actual: Row) = {
      expected.toSeq.zip(actual.toSeq).map { case(expectedRowField, actualRowField) =>
        diffStrings(Prettifier.default(expectedRowField), Prettifier.default(actualRowField))
      }
    }

    def diffStrings(expected: String, actual: String) = {
      if (expected != actual) {
        s"${expected} -/+ ${actual}"
      } else {
        expected
      }
    }

    def closestMatch(rows: Array[Row], similarTo: Row): Option[Row] = {
      rows.sortBy { row =>
        row.values.zip(similarTo.values).count { case(rowField, similarToField) =>
          rowField != similarToField
        }
      }.headOption
    }

    def missingRow(size: Int): Row = {
      Row.fromSeq(1 to size map { _ => null })
    }
  }
}
