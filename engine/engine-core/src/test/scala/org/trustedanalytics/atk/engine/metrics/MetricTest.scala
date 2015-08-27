package org.trustedanalytics.atk.engine.metrics

import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.engine.EngineConfig.{ metricStart, metricTick }

class MetricTest extends WordSpec with Matchers {
  "default metric auto start configuration" should {
    "be off" in {

      metricStart shouldEqual false
    }
  }

  "defalut metric tick configuration" should {
    "be 60 seconds" in {
      metricTick shouldEqual 60
    }
  }
}
