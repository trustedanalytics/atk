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
package org.trustedanalytics.atk.testutils

import java.io.File
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraph }
import org.scalatest.mock.MockitoSugar

/**
 * setup for testing export to OrientDB plugin functions
 */
trait TestingOrientDb {

  var tmpDir: File = null
  var dbUri: String = null
  var dbName: String = "OrientDbTest"
  var orientMemoryGraph: OrientGraph = null
  var orientFileGraph: OrientGraph = null

  /**
   * create in memory Orient graph database
   */
  def setupOrientDbInMemory(): Unit = {
    val uuid = java.util.UUID.randomUUID.toString
    orientMemoryGraph = new OrientGraph("memory:OrientTestDb" + uuid)
  }

  /**
   *
   */
  def setupOrientDb(): Unit = {

    tmpDir = DirectoryUtils.createTempDirectory("orient-graph-for-unit-testing")
    dbUri = "plocal:/" + tmpDir.getAbsolutePath + "/" + dbName
    val orientDocDb: ODatabaseDocumentTx = new ODatabaseDocumentTx(dbUri)
    orientDocDb.create()
    orientFileGraph = new OrientGraph(orientDocDb)

  }

  /**
   * commit the transaction and close/drop the graph database
   */
  def cleanupOrientDb(): Unit = {
    try {
      if (orientFileGraph != null) {
        orientFileGraph.commit()
        orientFileGraph.drop()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }
  }

  /**
   * commit the transaction and close the graph database
   */
  def cleanupOrientDbInMemory(): Unit = {
    try {
      if (orientMemoryGraph != null) {
        orientMemoryGraph.commit()
      }
    }
    finally {
      orientMemoryGraph.shutdown()
    }
  }
}
