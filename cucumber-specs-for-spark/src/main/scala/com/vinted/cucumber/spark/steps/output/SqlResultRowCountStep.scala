package com.vinted.cucumber.spark.steps.output

import com.vinted.cucumber.spark.steps.SpecStep

class SqlResultRowCountStep extends SpecStep {
  Then("""^SQL query "(.*?)" returns (\d+) rows""") {
    (sqlQuery: String, expectedCount: Long) => {
      context.sqlContext.sql(sqlQuery).count shouldEqual expectedCount
    }
  }
}
