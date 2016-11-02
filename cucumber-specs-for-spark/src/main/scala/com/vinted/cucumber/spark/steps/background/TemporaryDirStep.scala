package com.vinted.cucumber.spark.steps.background

import com.vinted.cucumber.spark.steps.SpecStep
import java.nio.file.Files

class TemporaryDirStep extends SpecStep {
  Given("""^temporary dir(?:ectory)? \(with path in \$\{(.*?)\}\)$""") {
    (tmpDirVarName: String) => {
      context.withVariable(tmpDirVarName, createTempDir())
    }
  }

  private

  def createTempDir() = Files.createTempDirectory(getClass.getName).toString
}
