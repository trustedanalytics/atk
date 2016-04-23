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
package org.trustedanalytics.atk.plugins.orientdb

import java.io.File

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.testutils.DirectoryUtils

/**
 * scala unit tests for GraphDbFactory: the first,checking creating OrientDB, the second, checking opening an existing graph
 */

class GraphDbFactoryTest extends WordSpec with Matchers {

  "graph database factory" should {
    "creates a graph database and takes input arguments" in {
      val dbUri: String = "memory:OrientTestDb"
      val userName: String = "admin"
      val password: String = "admin"
      val dbConfig = new DbConfigurations(dbUri, userName, password)
      val graph: OrientGraph = GraphDbFactory.createGraphDb(dbConfig)
      graph.isClosed shouldBe false
      graph.shutdown()
    }
    "connect to OrientDB" in {
      var tmpDir: File = null
      val dbName = "OrientDbTest"
      tmpDir = DirectoryUtils.createTempDirectory("orient-graph-for-unit-testing")
      val dbUri = "plocal:/" + tmpDir.getAbsolutePath + "/" + dbName
      val userName: String = "admin"
      val password: String = "admin"
      val dbConfig = new DbConfigurations(dbUri, userName, password)
      val orientDb = GraphDbFactory.graphDbConnector(dbName, dbConfig)
      orientDb.isClosed shouldBe false
      orientDb.shutdown()
      val orientDbnew = GraphDbFactory.graphDbConnector(dbName, dbConfig)
      orientDbnew.isClosed shouldBe false
      orientDbnew.drop()
      orientDbnew.isClosed shouldBe true

      //clean up: delete the database direcotry
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }
  }
}
