/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class ARIMAXModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "ARIMAXTrainArgs" should "be created with valid arguments" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val keyCol = "key"
    val timeseriesCol = "values"
    val xColumns = List("temperature", "humidity")

    val trainArgs = ARIMAXTrainArgs(modelRef, frameRef, timeseriesCol, xColumns, 1, 1, 1, 0)

  }

  it should "throw an exception when the time series column name is empty or null" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val keyCol = "key"
    val xColumns = List("temperature", "humidity")

    intercept[IllegalArgumentException] {
      ARIMAXTrainArgs(modelRef, frameRef, "", xColumns, 1, 1, 1, 0)
    }

    intercept[IllegalArgumentException] {
      ARIMAXTrainArgs(modelRef, frameRef, null, xColumns, 1, 1, 1, 0)
    }
  }

  it should "thrown an exception when the exogenous value column list is empty or null" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val keyCol = "key"
    val timeseriesCol = "values"
    val xColumns = List()

    intercept[IllegalArgumentException] {
      ARIMAXTrainArgs(modelRef, frameRef, timeseriesCol, xColumns, 1, 1, 1, 0)
    }

    intercept[IllegalArgumentException] {
      ARIMAXTrainArgs(modelRef, frameRef, timeseriesCol, null, 1, 1, 1, 0)
    }
  }

}

