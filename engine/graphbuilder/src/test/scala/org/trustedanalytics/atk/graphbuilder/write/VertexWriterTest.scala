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

package org.trustedanalytics.atk.graphbuilder.write

import org.mockito.Mockito._
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.graphbuilder.write.dao.VertexDAO
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex
import org.scalatest.mock.MockitoSugar

class VertexWriterTest extends WordSpec with Matchers with MockitoSugar {

  "VertexWriter" should {

    "support append true" in {
      // setup mocks
      val vertexDAO = mock[VertexDAO]
      val vertex = mock[GBVertex]

      // instantiated class under test
      val vertexWriter = new VertexWriter(vertexDAO, append = true)

      // invoke method under test
      vertexWriter.write(vertex)

      // validate
      verify(vertexDAO).updateOrCreate(vertex)
    }

    "support append false" in {
      // setup mocks
      val vertexDAO = mock[VertexDAO]
      val vertex = mock[GBVertex]

      // instantiate class under test
      val vertexWriter = new VertexWriter(vertexDAO, append = false)

      // invoke method under test
      vertexWriter.write(vertex)

      // validate
      verify(vertexDAO).create(vertex)
    }
  }

}
