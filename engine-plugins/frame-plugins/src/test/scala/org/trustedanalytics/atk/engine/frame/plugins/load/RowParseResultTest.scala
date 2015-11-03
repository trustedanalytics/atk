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


package org.trustedanalytics.atk.engine.frame.plugins.load

import org.scalatest.WordSpec
import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.RowParseResult

class RowParseResultTest extends WordSpec {

  "RowParseResult" should {
    "validate correct size of error frames" in {
      intercept[IllegalArgumentException] { RowParseResult(parseSuccess = false, Array[Any](1, 2, 3)) }
    }

    "not throw errors when error frame is correct size" in {
      RowParseResult(parseSuccess = false, Array[Any]("line", "error"))
    }

    "not validate for success frames" in {
      RowParseResult(parseSuccess = true, Array[Any]("any", "number", "of", "params", "is", "okay"))
    }
  }
}
