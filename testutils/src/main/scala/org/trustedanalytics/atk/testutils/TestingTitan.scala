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

import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph
import com.tinkerpop.blueprints.{ Graph, Vertex }
import org.apache.commons.configuration.BaseConfiguration

/**
 * This trait can be mixed into Tests to get a TitanGraph backed by Berkeley for testing purposes.
 *
 * The TitanGraph can be wrapped by IdGraph to allow tests to create vertices and edges with specific Ids.
 *
 * IMPORTANT! only one thread can use the graph below at a time. This isn't normally an issue because
 * each test usually gets its own copy.
 *
 * IMPORTANT! You must call cleanupTitan() when you are done
 */

trait TestingTitan {
  LogUtils.silenceTitan()

  var tmpDir: File = null
  var titanBaseConfig: BaseConfiguration = null
  var titanGraph: TitanGraph = null
  var titanIdGraph: Graph = null //ID graph is used for setting user-specified vertex Ids

  def setupTitan(): Unit = {
    tmpDir = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

    titanBaseConfig = new BaseConfiguration()
    titanBaseConfig.setProperty("storage.backend", "berkeleyje")
    titanBaseConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

    // Setting batch-loading to true to prevent uniqueness checks when appending to graph
    // Batch-loading disables automatic schema creation so the schema must be explictly defined
    titanBaseConfig.setProperty("storage.batch-loading", "true")

    //Trying to fix OutOfMemory errors during builds
    titanBaseConfig.setProperty("cache.tx-cache-size", 100)
    titanBaseConfig.setProperty("cache.db-cache", false)

    titanGraph = TitanFactory.open(titanBaseConfig)
    titanIdGraph = setupIdGraph()
    titanGraph.commit()
  }

  def setupIdGraph(): IdGraph[TitanGraph] = {
    val graphManager = titanGraph.getManagementSystem()
    val idKey = graphManager.makePropertyKey(IdGraph.ID).dataType(classOf[java.lang.Long]).make()
    graphManager.buildIndex(IdGraph.ID, classOf[Vertex]).addKey(idKey).buildCompositeIndex()
    graphManager.commit()
    new IdGraph(titanGraph, true, false)

  }

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (titanGraph != null) {
        titanGraph.shutdown()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }

    // make sure this class is unusable when we're done
    titanBaseConfig = null
    titanGraph = null
    tmpDir = null
    titanIdGraph = null
  }

}
