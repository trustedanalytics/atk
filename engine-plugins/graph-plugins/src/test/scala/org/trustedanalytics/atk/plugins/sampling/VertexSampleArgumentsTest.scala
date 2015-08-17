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

package org.trustedanalytics.atk.plugins.sampling

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.graph.GraphReference

class VertexSampleArgumentsTest extends WordSpec with MockitoSugar {

  "VertexSampleArguments" should {
    "require sample size greater than or equal to 1" in {
      VertexSampleArguments(mock[GraphReference], 1, "uniform")
    }

    "require sample size greater than 0" in {
      intercept[IllegalArgumentException] { VertexSampleArguments(mock[GraphReference], 0, "uniform") }
    }

    "require a valid sample type" in {
      intercept[IllegalArgumentException] { VertexSampleArguments(mock[GraphReference], 2, "badvalue") }
    }
  }
}
