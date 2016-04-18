package org.trustedanalytics.atk.plugins.orientdb.g

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.plugins.orientdb.GraphDbFactory

/**
 * Created by wtaie on 4/1/16.
 */
class GraphDbFactoryTest extends WordSpec with Matchers {

  "graph database factory" should {

    "creates a graph database and takes input arguments" in {
      val dbUri: String = "memory:OrientTestDb"
      val userName: String = "admin"
      val password: String = "admin"
      val graph: OrientGraph = GraphDbFactory.createGraphDb(dbUri, userName, password)
      graph.isClosed shouldBe false
      graph.shutdown()
    }
    "connect to OrientDB" in {
      val dbUri: String = "plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbTest"
      val orientDb = GraphDbFactory.GraphDbConnector(dbUri)
      orientDb.isClosed shouldBe false
      orientDb.shutdown()
      val orientDbnew = GraphDbFactory.GraphDbConnector(dbUri)
      orientDbnew.isClosed shouldBe false
      orientDbnew.drop()
      orientDbnew.isClosed shouldBe true
    }
  }
}
