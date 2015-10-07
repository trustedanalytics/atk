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

import com.typesafe.config.{ ConfigException, ConfigFactory, Config }
import org.scalatest.WordSpec

class SearchPathTest extends WordSpec {

  val searchPathWithOne = new SearchPath("src/test/resources/test-module")

  val searchPathWithNone = new SearchPath("src/test/resources/does-not-exist")

  val searchPathWithTwo = new SearchPath("src/test/resources/test-module:src/main/resources:src/test/resources/does-not-exist")

  val searchPathWithThree = new SearchPath("src/test/resources/does-not-exist:src/test/resources/valid-parent-modules:src/test/resources/fake-lib")

  val searchPathWithFour = new SearchPath("src/test/resources/does-not-exist"
    + ":src/test/resources/valid-parent-modules"
    + ":src/test/resources/fake-lib"
    + ":src/test/resources/test-module/test-module.jar")

  "SearchPath" should {

    "find a test module" in {
      assert(searchPathWithOne.findModules().head.getName == "test-module.jar")
    }

    "find a test module jar" in {
      assert(searchPathWithOne.findJars(Seq("test-module.jar")).length == 1)
    }

    "find expected 3 modules" in {
      val modules = searchPathWithThree.findModules()
      assert(modules.size == 3)
    }

    "find expected 3 jars" in {
      val jars = searchPathWithThree.findJars(Seq("a.jar", "b.jar", "c.jar"))
      assert(jars.length == 3)
      assert(jars(0).getFile.endsWith("a.jar"))
      assert(jars(1).getFile.endsWith("b.jar"))
      assert(jars(2).getFile.endsWith("c.jar"))
    }

    "find expected 4 modules" in {
      val modules = searchPathWithFour.findModules()
      assert(modules.size == 4)
    }

    "find expected 4 jars in any order" in {
      val jars = searchPathWithFour.findJars(Seq("c.jar", "test-module.jar", "a.jar", "b.jar"))
      assert(jars.length == 4)
      assert(jars(0).getFile.endsWith("c.jar"), s"not expected file ${jars(0)}")
      assert(jars(1).getFile.endsWith("test-module.jar"), s"not expected file ${jars(1)}")
      assert(jars(2).getFile.endsWith("a.jar"), s"not expected file ${jars(2)}")
      assert(jars(3).getFile.endsWith("b.jar"), s"not expected file ${jars(3)}")
    }

    "error when jar is missing" in {
      intercept[RuntimeException] {
        searchPathWithOne.findJars(Seq("does-not-exist.jar"))
      }
    }

    "error when one of two jars is missing" in {
      intercept[RuntimeException] {
        searchPathWithOne.findJars(Seq("test-module.jar", "does-not-exist.jar"))
      }
    }

    "not mind if path does not exist" in {
      assert(searchPathWithNone.findModules().isEmpty)
    }

    "be able to find modules under different paths" in {
      assert(searchPathWithTwo.findModules().length == 2)
    }
  }

}
