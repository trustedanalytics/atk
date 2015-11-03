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

package org.trustedanalytics.atk.rest.v1.decorators

import org.scalatest.{ Matchers, FlatSpec }
import org.trustedanalytics.atk.rest.v1.viewmodels.RelLink
import org.joda.time.DateTime
import org.trustedanalytics.atk.domain.command.Command
import org.trustedanalytics.atk.engine.{ TaskProgressInfo, ProgressInfo }

class CommandDecoratorTest extends FlatSpec with Matchers {

  val uri = "http://www.example.com/commands"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val command = new Command(1, "name", None, "", None, List(ProgressInfo(20.00f, Some(TaskProgressInfo(0)))), false, None, new DateTime, new DateTime)

  "CommandDecorator" should "be able to decorate a command" in {
    val decoratedCommand = CommandDecorator.decorateEntity(null, relLinks, command)
    decoratedCommand.id should be(1)
    decoratedCommand.name should be("name")
    decoratedCommand.links.head.uri should be("http://www.example.com/commands")
  }

  it should "set the correct URL in decorating a list of commands" in {
    val commandHeaders = CommandDecorator.decorateForIndex(uri, Seq(command))
    val commandHeader = commandHeaders.toList.head
    commandHeader.url should be("http://www.example.com/commands/1")
  }
}
