package com.vinted.cucumber.spark.spec

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.test.TestHiveContext
import scala.collection.JavaConversions._

object TestHive extends TestHiveContext(TestSparkContextProvider.sparkContext)

object TestSparkContextProvider {
  lazy val log = Logger.getLogger(getClass.getName)

  lazy val sparkContext = {
    log.info(s"TestHive config: ${sparkProperties}")
    new SparkContext(master, appName, sparkConf)
  }

  private

  val DEFAULT_TEST_CONFIGURATION = Map(
    "spark.master" -> "local[2]",
    "spark.app.name" -> "TestSQLContext",
    "spark.sql.test" -> "",
    "spark.ui.enabled" -> "false",
    "spark.default.parallelism" -> "2",
    "spark.sql.shuffle.partitions" -> "2",
    "spark.sql.hive.metastore.barrierPrefixes" -> "org.apache.spark.sql.hive.execution.PairSerDe"
  )

  lazy val master: String = {
    sparkProperties("spark.master")
  }

  lazy val appName: String = {
    sparkProperties("spark.app.name")
  }

  lazy val sparkConf: SparkConf = {
    sparkProperties.foldLeft(new SparkConf()) { case (conf, (property, value)) =>
      conf.set(property, value)
    }
  }

  lazy val sparkProperties: Map[String, String] = {
    DEFAULT_TEST_CONFIGURATION ++ givenSparkProperties
  }

  lazy val givenSparkProperties: Map[String, String] = {
    System.getProperties().toMap.filter { case (property, value) =>
      property.startsWith("spark.")
    }
  }
}
