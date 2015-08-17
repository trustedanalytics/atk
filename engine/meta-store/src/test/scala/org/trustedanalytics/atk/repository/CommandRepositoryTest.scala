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

package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.command.CommandTemplate
import org.scalatest.Matchers
import org.trustedanalytics.atk.engine.{ ProgressInfo, TaskProgressInfo }

class CommandRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "CommandRepository" should "be able to create commands" in {

    val commandRepo = slickMetaStoreComponent.metaStore.commandRepo

    slickMetaStoreComponent.metaStore.withSession("command-test") {
      implicit session =>

        val name = "my-name"

        // create a command
        val command = commandRepo.insert(new CommandTemplate(name, None))
        command.get.name shouldBe name

        // look it up and validate expected values
        val command2 = commandRepo.lookup(command.get.id)
        command.get shouldBe command2.get
        command2.get.name shouldBe name
        command2.get.createdOn should not be null
        command2.get.modifiedOn should not be null
    }
  }

  "CommandRepository" should "update single column for single row" in {
    val commandRepo = slickMetaStoreComponent.metaStore.commandRepo

    slickMetaStoreComponent.metaStore.withSession("command-test") {
      implicit session =>

        val name = "my-name"

        // create a command
        val command = commandRepo.insert(new CommandTemplate(name, None))
        commandRepo.updateProgress(command.get.id, List(ProgressInfo(100, Some(TaskProgressInfo(5)))))
        val command2 = commandRepo.lookup(command.get.id)
        command2.get.progress shouldBe List(ProgressInfo(100, Some(TaskProgressInfo(5))))
    }
  }

  "CommandRepository" should "not update progress if command is complete" in {
    val commandRepo = slickMetaStoreComponent.metaStore.commandRepo

    slickMetaStoreComponent.metaStore.withSession("command-test") {
      implicit session =>

        val name = "my-name"

        // create a command
        val command = commandRepo.insert(new CommandTemplate(name, None))
        commandRepo.updateProgress(command.get.id, List(ProgressInfo(100, Some(TaskProgressInfo(5))), ProgressInfo(50, Some(TaskProgressInfo(5)))))
        val command2 = commandRepo.lookup(command.get.id)
        commandRepo.update(command2.get.copy(complete = true, progress = List(ProgressInfo(100, Some(TaskProgressInfo(5))), ProgressInfo(50, Some(TaskProgressInfo(5))))))

        commandRepo.updateProgress(command.get.id, List(ProgressInfo(40, Some(TaskProgressInfo(5))), ProgressInfo(70, Some(TaskProgressInfo(7)))))
        val command3 = commandRepo.lookup(command.get.id)
        command3.get.progress shouldBe List(ProgressInfo(100, Some(TaskProgressInfo(5))), ProgressInfo(50, Some(TaskProgressInfo(5))))
    }
  }

}
