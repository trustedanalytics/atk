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

package org.trustedanalytics.atk.rest.v1

import org.trustedanalytics.atk.domain.UserPrincipal
import org.trustedanalytics.atk.domain.model.ModelEntity
import org.trustedanalytics.atk.engine.plugin.{ Call, Invocation }
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import org.mockito.Mockito._

import org.trustedanalytics.atk.engine.Engine
import scala.concurrent.Future
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.rest.{ ServiceTest, CommonDirectives }
import org.trustedanalytics.atk.domain.schema.Schema
import org.joda.time.DateTime

class ModelEntityServiceTest extends ServiceTest {
  implicit val userPrincipal = mock[UserPrincipal]
  implicit val call: Invocation = Call(userPrincipal, SprayExecutionContext.global, "fakeClientId")
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("models")).thenReturn(provide(call))

  "ModelService" should "give an empty set when there are no models" in {
    val engine = mock[Engine]
    val modelService = new ModelService(commonDirectives, engine)

    when(engine.getModels()).thenReturn(Future.successful(Seq()))

    Get("/models") ~> modelService.modelRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

}
