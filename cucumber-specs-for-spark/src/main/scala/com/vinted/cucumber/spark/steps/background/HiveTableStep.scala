package com.vinted.cucumber.spark.steps.background

import com.vinted.cucumber.spark.steps.SpecStep
import com.vinted.cucumber.spark.ext.DataTableExt._
import cucumber.api.DataTable
import org.apache.spark.sql.DataFrame

class HiveTableStep extends SpecStep {
  Given("""^Hive table "(\w+)\.(\w+)":?""") {
    (databaseName: String, tableName: String, data: DataTable) => {
      saveHiveTable(data.toDataFrame, databaseName, tableName)
    }
  }

  private

  def saveHiveTable(df: DataFrame, databaseName: String, tableName: String) = {
    val tmpTableName = s"tmp_spark_${databaseName}__${tableName}"

    df.registerTempTable(tmpTableName)
    sql(s"CREATE DATABASE IF NOT EXISTS `${databaseName}`")
    sql(s"DROP TABLE IF EXISTS `${databaseName}`.`${tableName}`")
    sql(s"CREATE TABLE `${databaseName}`.`${tableName}` AS SELECT * FROM `${tmpTableName}`")
    df.sqlContext.dropTempTable(tmpTableName)
  }
}
