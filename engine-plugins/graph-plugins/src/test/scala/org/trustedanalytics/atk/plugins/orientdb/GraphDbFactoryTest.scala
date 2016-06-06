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
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphNoTx, OrientGraph }
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.testutils.DirectoryUtils

class GraphDbFactoryTest extends WordSpec with Matchers {

  "graph database factory" should {
    val userName: String = "admin"
    val password: String = "admin"
    val host = "host"
    val port = "port"
    val rootPassword = "root"

    "creates a graph database and takes input arguments" in {
      val dbUri: String = "memory:OrientTestDb"
      val dbConfig = new DbConfiguration(dbUri, userName, password, port, host, rootPassword, false)
      //Tested method
      val graph: OrientGraphNoTx = GraphDbFactory.createGraphDb(dbConfig)
      //Results validation
      graph.isClosed shouldBe false
      graph.shutdown()
      graph.isClosed shouldBe true
    }

    "connect to OrientDB" in {
      var tmpDir: File = null
      val dbName = "OrientDbTest"
      tmpDir = DirectoryUtils.createTempDirectory("orient-graph-for-unit-testing")
      val dbUri = "plocal:" + tmpDir.getAbsolutePath + "/" + dbName
      val dbConfig = new DbConfiguration(dbUri, userName, password, port, host, rootPassword, false)
      //Tested method
      val orientDb = GraphDbFactory.graphDbConnector(dbConfig)
      //Results validation
      orientDb.isClosed shouldBe false
      orientDb.shutdown()
      orientDb.isClosed shouldBe true

      //clean up: delete the database directory
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }
  }
}
