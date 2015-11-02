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


package org.trustedanalytics.atk.graphbuilder.util

import org.scalatest.{ Matchers, WordSpec }

class StringUtilsTest extends WordSpec with Matchers {

  "StringUtils.nullSafeToString()" should {

    "handle null objects" in {
      StringUtils.nullSafeToString(null) shouldBe null
    }

    "handle Strings" in {
      StringUtils.nullSafeToString("anyString") shouldBe "anyString"
    }

    "handle Ints" in {
      StringUtils.nullSafeToString(1) shouldBe "1"
    }

    "handle complex objects" in {
      StringUtils.nullSafeToString(Map("my-key" -> "my-value")) shouldBe "Map(my-key -> my-value)"
    }
  }
}
