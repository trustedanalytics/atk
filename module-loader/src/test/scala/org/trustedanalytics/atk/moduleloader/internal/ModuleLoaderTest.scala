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

package org.trustedanalytics.atk.moduleloader.internal

import org.scalatest.WordSpec
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class ModuleLoaderTest extends WordSpec with MockitoSugar {

  /**
   * Create a mock ModuleConfig for testing
   */
  def mockModuleConfig(name: String, parent: Option[String] = None, memberOf: Option[String] = None): ModuleConfig = {
    val moduleConfig = mock[ModuleConfig]
    when(moduleConfig.name).thenReturn(name)
    when(moduleConfig.parentName).thenReturn(parent)
    when(moduleConfig.memberOf).thenReturn(memberOf)
    moduleConfig
  }

  "ModuleLoader" should {

    "allow valid parent names" in {

      val moduleLoader = new ModuleLoader(mock[SearchPath])

      val c1 = mockModuleConfig("c1", parent = Some("c2"))
      val c2 = mockModuleConfig("c2", parent = Some("c3"))
      val c3 = mockModuleConfig("c3")

      // valid
      moduleLoader.validate(Seq(c1, c2, c3))

      // order doesn't matter
      moduleLoader.validate(Seq(c2, c1, c3))
      moduleLoader.validate(Seq(c3, c2, c1))
    }

    "catch non-existent parent names" in {

      val moduleLoader = new ModuleLoader(mock[SearchPath])

      val c1 = mockModuleConfig("c1", parent = Some("c2"))
      val c2 = mockModuleConfig("c2", parent = Some("c3"))
      val c3 = mockModuleConfig("c3", parent = Some("does-not-exist"))

      intercept[IllegalArgumentException] {
        moduleLoader.validate(Seq(c1, c2, c3))
      }
    }

    "allow valid memberOf names" in {

      val moduleLoader = new ModuleLoader(mock[SearchPath])

      val c1 = mockModuleConfig("c1", parent = Some("c2"))
      val c2 = mockModuleConfig("c2")
      val c3 = mockModuleConfig("c3", memberOf = Some("c2"))

      // valid
      moduleLoader.validate(Seq(c1, c2, c3))

      // order doesn't matter - still valid
      moduleLoader.validate(Seq(c2, c1, c3))
      moduleLoader.validate(Seq(c3, c2, c1))
    }

    "catch non-existent memberOf names" in {

      val moduleLoader = new ModuleLoader(mock[SearchPath])

      val c1 = mockModuleConfig("c1")
      val c2 = mockModuleConfig("c2")
      val c3 = mockModuleConfig("c3", memberOf = Some("does-not-exist"))

      intercept[IllegalArgumentException] {
        moduleLoader.validate(Seq(c1, c2, c3))
      }
    }

    "allow valid parent references" in {
      val moduleLoader = new ModuleLoader(new SearchPath("src/test/resources/valid-parent-modules:src/main/resources"))

      val modules = moduleLoader.load()
      assert(modules.values.size == 4)
      assert(modules("module-a").parentName == Option("module-c"))
      assert(modules("module-b").parentName == Option("module-c"))
      assert(modules("module-c").parentName == Option("system"))
      assert(modules("system").parentName.isEmpty)

      // validate the classloader parent relationship got setup right
      assert(modules("module-a").classLoader.getParent == modules("module-c").classLoader)
      assert(modules("module-b").classLoader.getParent == modules("module-c").classLoader)
      assert(modules("module-c").classLoader.getParent == this.getClass.getClassLoader)
    }

    "catch circular parent references" in {
      val moduleLoader = new ModuleLoader(new SearchPath("src/test/resources/circular-parent-modules"))

      intercept[IllegalArgumentException] {
        moduleLoader.load()
      }
    }

    "catch multi-level member-of references" in {
      val moduleLoader = new ModuleLoader(new SearchPath("src/test/resources/multi-level-member-of"))

      intercept[IllegalArgumentException] {
        moduleLoader.load()
      }
    }

    "allow valid member-of" in {
      val moduleLoader = new ModuleLoader(new SearchPath("src/test/resources/valid-member-of:src/main/resources:src/test/resources/fake-lib"))

      val modules = moduleLoader.load()
      assert(modules.values.size == 2)
      assert(modules("module-c").parentName == Option("system"))
      assert(modules("system").parentName.isEmpty)
      assert(modules("module-c").jarNames.contains("c.jar"), "c.jar not found")
      assert(modules("module-c").jarNames.contains("a.jar"), "a.jar not found")
      assert(modules("module-c").jarNames.contains("b.jar"), "b.jar not found")
    }

    "combines member-of configs correctly" in {
      val moduleLoader = new ModuleLoader(new SearchPath("src/test/resources/valid-member-of:src/main/resources"))

      val moduleConfigs = moduleLoader.loadModuleConfigs()
      assert(moduleConfigs.size == 2)
      assert(moduleConfigs.head.jarNames.contains("c.jar"), "c.jar not found")
      assert(moduleConfigs.head.jarNames.contains("a.jar"), "a.jar not found")
      assert(moduleConfigs.head.jarNames.contains("b.jar"), "b.jar not found")
    }
  }
}
