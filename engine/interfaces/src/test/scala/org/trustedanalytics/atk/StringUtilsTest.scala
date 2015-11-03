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


package org.trustedanalytics.atk

import org.scalatest.WordSpec

class StringUtilsTest extends WordSpec {

  "StringUtils" should {

    "be true for alpha-numeric with underscores" in {
      assert(StringUtils.isAlphanumericUnderscore("a"))
      assert(StringUtils.isAlphanumericUnderscore("abc"))
      assert(StringUtils.isAlphanumericUnderscore("abc_def"))
      assert(StringUtils.isAlphanumericUnderscore("MixedCase"))
      assert(StringUtils.isAlphanumericUnderscore("Mixed_Case_With_Underscores"))
      assert(StringUtils.isAlphanumericUnderscore("_"))
      assert(StringUtils.isAlphanumericUnderscore("___"))
      assert(StringUtils.isAlphanumericUnderscore(""))
    }

    "be false for non- alpha-numeric with underscores" in {
      assert(!StringUtils.isAlphanumericUnderscore("per%cent"))
      assert(!StringUtils.isAlphanumericUnderscore("spa ce"))
      assert(!StringUtils.isAlphanumericUnderscore("doll$ar"))
      assert(!StringUtils.isAlphanumericUnderscore("pound#"))
      assert(!StringUtils.isAlphanumericUnderscore("period."))
      assert(!StringUtils.isAlphanumericUnderscore("explanation!"))
      assert(!StringUtils.isAlphanumericUnderscore("hy-phen"))
    }

  }

}
