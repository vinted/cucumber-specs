package com.vinted.cucumber.scala.steps.hooks

import com.vinted.cucumber.spark.spec.Context
import com.vinted.cucumber.spark.steps.SpecStep

class AfterScenario extends SpecStep {
  After() { specification =>
    Context.reset()
  }
}
