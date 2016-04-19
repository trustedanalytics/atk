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
package org.trustedanalytics.atk.plugins.orientdb.g

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.plugins.orientdb.GraphDbFactory

/**
 * Created by wtaie on 4/1/16.
 */
class GraphDbFactoryTest extends WordSpec with Matchers {

  "graph database factory" should {
    val graphFactory = new GraphDbFactory
    "creates a graph database and takes input arguments" in {
      val dbUri: String = "memory:OrientTestDb"
      val userName: String = "admin"
      val password: String = "admin"
      val graph: OrientGraph = graphFactory.createGraphDb(dbUri, userName, password)
      graph.isClosed shouldBe false
      graph.shutdown()
    }
    "connect to OrientDB" in {
      val dbUri: String = "plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbTest"
      val graphFactory = new GraphDbFactory
      val orientDb = graphFactory.GraphDbConnector(dbUri)
      orientDb.isClosed shouldBe false
      orientDb.shutdown()
      val orientDbnew = graphFactory.GraphDbConnector(dbUri)
      orientDbnew.isClosed shouldBe false
      orientDbnew.drop()
      orientDbnew.isClosed shouldBe true
    }
  }
}
