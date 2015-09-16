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

package org.trustedanalytics.atk.graphbuilder.write.titan

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }
import org.trustedanalytics.atk.graphbuilder.write.VertexWriter
import com.thinkaurelius.titan.core.TitanVertex

class TitanVertexWriterTest extends WordSpec with Matchers with MockitoSugar {

  "TitanVertexWriter" should {

    "use the underlying writer and populate the gbIdToPhysicalId mapping" in {
      // setup mocks
      val vertexWriter = mock[VertexWriter]
      val gbVertex = mock[GBVertex]
      val titanVertex = mock[TitanVertex]
      val gbId = mock[Property]
      val physicalId = new java.lang.Long(123)

      when(gbVertex.gbId).thenReturn(gbId)
      when(titanVertex.getLongId()).thenReturn(physicalId)
      when(vertexWriter.write(gbVertex)).thenReturn(titanVertex)

      // instantiated class under test
      val titanVertexWriter = new TitanVertexWriter(vertexWriter)

      // invoke method under test
      val gbIdToPhysicalId = titanVertexWriter.write(gbVertex)

      // validate
      verify(vertexWriter).write(gbVertex)
      gbIdToPhysicalId.gbId shouldBe gbId
      gbIdToPhysicalId.physicalId shouldBe physicalId
    }

  }

}
