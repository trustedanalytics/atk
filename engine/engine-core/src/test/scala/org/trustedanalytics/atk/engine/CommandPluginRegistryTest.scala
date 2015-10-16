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

package org.trustedanalytics.atk.engine

import org.scalatest.{ Matchers, FlatSpec }
import org.mockito.Mockito._
import org.trustedanalytics.atk.engine.command.{ CommandPluginRegistry, CommandLoader }
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.trustedanalytics.atk.moduleloader.Module
import org.scalatest.mock.MockitoSugar

class CommandPluginRegistryTest extends FlatSpec with Matchers with MockitoSugar {

  "plugin registry initialization" should "load from the loader" in {
    val loader = mock[CommandLoader]
    val mockPlugin = mock[CommandPlugin[Product, Product]]
    val mockResults = List((Option(mock[Module]), mockPlugin)).toIterable

    when(mockPlugin.name).thenReturn("mock-plugin")
    when(loader.loadFromConfig()).thenReturn(mockResults)
    val registry = new CommandPluginRegistry(loader)
    registry.getCommandPlugin("mock-plugin") shouldBe Some(mockPlugin)
    registry.getCommandPlugin("not exists") shouldBe None
  }

  "plugin" should "return module name" in {
    val loader = mock[CommandLoader]
    val mockPlugin = mock[CommandPlugin[Product, Product]]
    val mockModule = mock[Module]
    val mockResults = List((Option(mockModule), mockPlugin)).toIterable

    when(mockPlugin.name).thenReturn("mock-plugin")
    when(mockModule.name).thenReturn("mock-module")
    when(loader.loadFromConfig()).thenReturn(mockResults)

    val registry = new CommandPluginRegistry(loader)
    registry.moduleNameForPlugin("mock-plugin") shouldBe "mock-module"
  }
}
