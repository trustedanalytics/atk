package org.trustedanalytics.atk.plugins.orientdb.g


import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.scalatest.{Matchers, WordSpec}
import org.trustedanalytics.atk.plugins.orientdb.GraphDbFactory

/**
  * Created by wtaie on 4/1/16.
  */
class GraphDbFactoryTest extends WordSpec with Matchers {

  "graph database factory" should {
    "creates a graph database and takes input arguments" in{
      val dbUrl: String = "memory:OrientTestDb"
      val userName: String = "admin"
      val password: String = "admin"
      val graph: OrientGraph = GraphDbFactory.createGraphDb(dbUrl,userName,password)
      graph.isClosed shouldBe false
      graph.shutdown()
    }
  }
}
