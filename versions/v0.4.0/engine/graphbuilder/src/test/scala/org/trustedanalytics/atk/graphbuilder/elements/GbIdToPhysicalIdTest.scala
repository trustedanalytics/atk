/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.graphbuilder.elements

import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class GbIdToPhysicalIdTest extends WordSpec with Matchers with MockitoSugar {

  "GbIdToPhysicalId" should {

    "be able to be converted to a tuple" in {
      val gbId = mock[Property]
      val physicalId = new java.lang.Long(4)
      val gbToPhysical = new GbIdToPhysicalId(gbId, physicalId)
      gbToPhysical.toTuple._1 shouldBe gbId
      gbToPhysical.toTuple._2 shouldBe physicalId
    }
  }
}
