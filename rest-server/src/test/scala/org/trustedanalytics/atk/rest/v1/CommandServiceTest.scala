/**
 *  Copyright (c) 2016 Intel Corporation 
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
import org.trustedanalytics.atk.engine.plugin.{ Invocation, Call }
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import org.mockito.Mockito._
import org.trustedanalytics.atk.rest.{ ServiceTest, CommonDirectives }
import org.trustedanalytics.atk.engine.Engine
import scala.concurrent.Future

class CommandServiceTest extends ServiceTest {

  implicit val userPrincipal = mock[UserPrincipal]
  implicit val call: Invocation = Call(userPrincipal, SprayExecutionContext.global, "fakeClientId")
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("commands")).thenReturn(provide(call))

  "CommandService" should "give an empty set when there are no results" in {

    val engine = mock[Engine]
    val commandService = new CommandService(commonDirectives, engine)

    when(engine.getCommands(0, 20)).thenReturn(Future.successful(Seq()))

    Get("/commands") ~> commandService.commandRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

}
