package org.trustedanalytics.atk.plugins.orientdb

import com.orientechnologies.orient.core.Orient
import com.tinkerpop.blueprints.GraphFactory.
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by wtaie on 4/1/16.
  */
class GraphDbFactoryTest extends WordSpec with Matchers {
  val dbUrl: String = "plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/testGraph54"
  "Graph factory" should {
    "creates orient graph database" in {
      val graphDb: OrientGraph = GraphFactory.
    }
  }


}
