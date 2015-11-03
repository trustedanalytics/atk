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


package org.trustedanalytics.atk.graphbuilder.elements

import org.scalatest.{ WordSpec, Matchers }

class PropertyTest extends WordSpec with Matchers {

  "Property" should {

    "merge 2 properties with the same key to 1" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyA", "valueA")

      val result = Property.merge(Set(p1), Set(p2))

      result shouldEqual Set(p1)
    }

    "merge 2 properties with different keys to 2" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyB", "valueB")

      val result = Property.merge(Set(p1), Set(p2))

      result shouldEqual Set(p1, p2)

    }

    "merge 7 properties with mixture of same/different keys to 5" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyB", "valueB")
      val p3 = new Property("keyC", "valueC")
      val p4 = new Property("keyB", "valueB2")
      val p5 = new Property("keyD", "valueD")
      val p6 = new Property("keyA", "valueA2")
      val p7 = new Property("keyE", "valueE")

      val result = Property.merge(Set(p1, p2, p3, p4), Set(p5, p6, p7))

      result.map({ case Property(key, value) => key }) shouldEqual Set("keyA", "keyB", "keyC", "keyD", "keyE")
    }

    "provide convenience constructor" in {
      val p = new Property(1, 2)
      p.key shouldBe "1" // key 1 is converted to a String
      p.value shouldBe 2
    }
  }
}
