package com.vinted.cucumber.spark.spec

trait Variables {
  var variables: Map[String, String] = Map()

  def withVariable(identifier: String, value: String) = {
    variables += identifier -> value
  }

  def replaceVariables(s: String) = {
    ensureNoMissingVariables(
      variables.foldLeft(s) { case (string, (variable, value)) =>
        string.replace("${" + variable + "}", value)
      }
    )
  }

  def resetVariables() = {
    variables = Map()
  }

  private

  val VARIABLE_REGEX = """\$\{(.*?)\}""".r

  def ensureNoMissingVariables(string: String) = {
    val undefinedVariables = VARIABLE_REGEX.findAllIn(string).toList

    if (undefinedVariables.isEmpty) {
      string
    } else {
      throw new RuntimeException(s"Undefined variable(s): ${undefinedVariables.mkString(", ")}")
    }
  }
}
