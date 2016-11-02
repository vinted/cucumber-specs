package com.vinted.cucumber.spark.steps.output

import com.vinted.cucumber.spark.ext.DataTableExt._
import com.vinted.cucumber.spark.ext.DataFrameExt._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.Matchers

trait Assertions extends Matchers {
  def assertIsSubset(expectedSubset: DataFrame, actual: DataFrame) = {
    // first make sure columns are present
    expectedSubset.columns.foreach { columnName =>
      actual.columns should contain(columnName)
    }

    val expectedColumns = expectedSubset.columns.map(col)
    val columnLimitedActual = actual.select(expectedColumns: _*)
    val intersection = expectedSubset.reducePrecision()
      .intersect(columnLimitedActual.reducePrecision())

    // expectedSubset.distict.count is used to support duplicated assertion lines e.g.
    // we have an actual entry ((1)) we expect ((1), (1)) and intersection is ((1))
    // intersection.count is less than expectedSubset count and we have to use distinct
    // intersection exists tests are not meant to match row counts.
    // example use case: user temporal dimension testing.
    if (intersection.count < expectedSubset.distinct.count) {
      fail(
        s"Unexpected result:\n" +
          s"Difference:\n${expectedSubset.diff(columnLimitedActual)}\n" +
          s"Expected:\n${expectedSubset.toAscii(1000)}\n" +
          s"Actual:\n${columnLimitedActual.toAscii(1000)}"
      )
    }
  }

  def assertNoIntersection(expectedToBeAbsent: DataFrame, actual: DataFrame) = {
    // first make sure columns are present
    expectedToBeAbsent.columns.foreach { columnName =>
      actual.columns should contain(columnName)
    }

    val columnLimitedActual = actual.select(expectedToBeAbsent.columns.map(col):_*)
    val intersection = expectedToBeAbsent.reducePrecision()
      .intersect(columnLimitedActual.reducePrecision())

    if (intersection.count > 0) {
      fail(
        s"Unexpected rows found:\n${intersection.toAscii(1000)}\nIn:\n${actual.toAscii(1000)}"
      )
    }
  }
}
