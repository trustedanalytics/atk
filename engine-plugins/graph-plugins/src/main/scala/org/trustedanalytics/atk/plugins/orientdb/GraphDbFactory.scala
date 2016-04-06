package org.trustedanalytics.atk.plugins.orientdb

import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientGraph}

/**
  * Created by wtaie on 4/1/16.
  * create Orient graph database (graph type is transactional)
  */
 object GraphDbFactory {
  def createGraphDb(dbUrl: String, userName: String, password: String): OrientGraph = {
    val factory: OrientGraphFactory = new OrientGraphFactory(dbUrl,userName,password)
    var graph: OrientGraph = factory.getTx
    return graph
  }

}
