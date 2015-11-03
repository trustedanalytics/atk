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

package org.trustedanalytics.atk.domain.frame

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

class ColumnSummaryStatisticsArgsTest extends WordSpec with MockitoSugar {

  "ColumnSummaryStatistics" should {
    "not allow null FrameReference" in {
      intercept[IllegalArgumentException] { ColumnSummaryStatisticsArgs(null, "dataColumn", None, None) }
    }
    "not allow null dataColumn" in {
      intercept[IllegalArgumentException] { ColumnSummaryStatisticsArgs(mock[FrameReference], null, None, None) }
    }
  }
}
