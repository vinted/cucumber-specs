package com.vinted.cucumber.spark.steps.output

import com.vinted.cucumber.spark.ext.DataTableExt._
import com.vinted.cucumber.spark.steps.SpecStep
import cucumber.api.DataTable

class SqlResultNoRowsStep extends SpecStep with Assertions {
  Then("""^SQL query "(.*?)" does not include(?: the following (?:rows|subset):?)""") {
    (sqlQuery: String, expectedToBeAbsent: DataTable) => {
      assertNoIntersection(expectedToBeAbsent.toDataFrame, context.sqlContext.sql(sqlQuery))
    }
  }
}
