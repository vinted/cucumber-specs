package com.vinted.cucumber.spark.steps.output

import com.vinted.cucumber.spark.ext.DataTableExt._
import com.vinted.cucumber.spark.steps.SpecStep
import cucumber.api.DataTable

class SqlResultRowsStep extends SpecStep with Assertions {
  Then("""^SQL query "(.*?)" includes(?: the following (?:rows|subset):?)""") {
    (sqlQuery: String, expectedTable: DataTable) => {
      assertIsSubset(expectedTable.toDataFrame, context.sqlContext.sql(sqlQuery))
    }
  }
}
