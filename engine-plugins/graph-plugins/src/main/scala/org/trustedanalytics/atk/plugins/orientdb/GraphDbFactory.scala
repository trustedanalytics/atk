package org.trustedanalytics.atk.plugins.orientdb

import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientGraph}

/**
  * Created by wtaie on 4/1/16.
  * create Orient graph database (graph type is transactional)
  */
class GraphDbFactory {
  def createGraphDb(dbUrl: String): OrientGraph = {
    val factory: OrientGraphFactory = new OrientGraphFactory(dbUrl)
    var graph: OrientGraph = factory.getTx
    return graph
  }

}
