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
package org.trustedanalytics.atk.moduleloader.internal

import java.io.File

import com.typesafe.config.{ ConfigException, ConfigFactory }
import org.scalatest.WordSpec

class ModuleConfigTest extends WordSpec {

  "ModuleConfig" should {

    "accept valid config" in {

      val config = ConfigFactory.parseString(
        """
          atk.module {

            name = "my-name"

            jar-names = [ "one.jar", "two.jar" ]

          }
        """)
      val moduleConfig = new ModuleConfig("location", config)
      assert(moduleConfig.name == "my-name")
      assert(moduleConfig.parentName.isEmpty)
      assert(moduleConfig.memberOf.isEmpty)
      assert(moduleConfig.jarNames == Seq("one.jar", "two.jar"))
    }

    "accept valid config with parent" in {

      val config = ConfigFactory.parseString(
        """
          atk.module {

            name = "my-name"

            parent = "my-parent"

            jar-names = [ "one.jar", "two.jar" ]

          }
        """)
      val moduleConfig = new ModuleConfig("location", config)
      assert(moduleConfig.name == "my-name")
      assert(moduleConfig.parentName.get == "my-parent")
      assert(moduleConfig.memberOf.isEmpty)
      assert(moduleConfig.jarNames == Seq("one.jar", "two.jar"))
    }

    "accept valid config with member-of" in {

      val config = ConfigFactory.parseString(
        """
          atk.module {

            name = "my-name"

            member-of = "some-module"

            jar-names = [ "one.jar", "two.jar" ]

          }
        """)
      val moduleConfig = new ModuleConfig("location", config)
      assert(moduleConfig.name == "my-name")
      assert(moduleConfig.parentName.isEmpty)
      assert(moduleConfig.memberOf.get == "some-module")
      assert(moduleConfig.jarNames == Seq("one.jar", "two.jar"))
    }

    "not allow both parent and member-of" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseString(
          """
          atk.module {

            name = "my-name"

            parent = "my-parent"

            // invalid to specify both parent and member-of
            member-of = "another"

            jar-names = [ "one.jar", "two.jar" ]

          }
          """)
        new ModuleConfig("location", config)
      }
    }

    "allow valid system config" in {
      val config = ConfigFactory.parseString(
        """
          atk.module {

            // 'system' is a special name reserved for the module-loader itself
            name = "system"

            jar-names = [ "one.jar", "two.jar" ]

          }
          """)
      val moduleConfig = new ModuleConfig("location", config)
      assert(moduleConfig.name == "system")
      assert(moduleConfig.parentName.isEmpty)
      assert(moduleConfig.memberOf.isEmpty)
      assert(moduleConfig.jarNames == Seq("one.jar", "two.jar"))
    }

    "not allow parent for system config" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseString(
          """
          atk.module {

            // 'system' is a special name reserved for the module-loader itself
            name = "system"

            parent = "my-parent"

            jar-names = [ "one.jar", "two.jar" ]

          }
          """)
        new ModuleConfig("location", config)
      }
    }

    "not allow member-of for system config" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseString(
          """
          atk.module {

            // 'system' is a special name reserved for the module-loader itself
            name = "system"

            member-of = "some-module"

            jar-names = [ "one.jar", "two.jar" ]

          }
          """)
        new ModuleConfig("location", config)
      }
    }

    "require modules to have a name" in {
      intercept[ConfigException] {
        new ModuleConfig("location", ConfigFactory.empty())
      }
    }

    "require modules to have a non-empty name" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseString(
          """
          atk.module.name = ""
          """)
        new ModuleConfig("location", config)
      }
    }

    "require modules to have a non-blank name" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseString(
          """
          atk.module.name = "  "
          """)
        new ModuleConfig("location", config)
      }
    }

    "be able to load a ModuleConfig from a file" in {
      val moduleFile = new File("src/test/resources/test-module/test-module.jar")
      val moduleConfig = ModuleConfig.loadModuleConfig(moduleFile)
      assert(moduleConfig.name == "test-module")
      assert(moduleConfig.parentName.isEmpty)
      assert(moduleConfig.memberOf.isEmpty)
      assert(moduleConfig.jarNames == Nil)
    }

    "be able to load a ModuleConfig from a file with other values" in {
      val moduleFile = new File("src/test/resources/valid-member-of/module-a.jar")
      val moduleConfig = ModuleConfig.loadModuleConfig(moduleFile)
      assert(moduleConfig.name == "module-a")
      assert(moduleConfig.parentName.isEmpty)
      assert(moduleConfig.memberOf == Option("module-c"))
      assert(moduleConfig.jarNames == Seq("a.jar"))
    }

    "be able to combine configs" in {
      val config1 = ConfigFactory.parseString(
        """
          exampleA = 1
          exampleC = 1
        """)

      val config2 = ConfigFactory.parseString(
        """
          exampleA = 2
          exampleB = 2
        """)

      val combinedConfig = ModuleConfig.combineConfigs(Seq(config1, config2))
      assert(combinedConfig.getInt("exampleA") == 1)
      assert(combinedConfig.getInt("exampleB") == 2)
      assert(combinedConfig.getInt("exampleC") == 1)

      val combinedInDiffOrder = ModuleConfig.combineConfigs(Seq(config2, config1))
      assert(combinedInDiffOrder.getInt("exampleA") == 2)
      assert(combinedInDiffOrder.getInt("exampleB") == 2)
      assert(combinedInDiffOrder.getInt("exampleC") == 1)
    }

  }
}
