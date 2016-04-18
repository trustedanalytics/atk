package org.trustedanalytics.atk.plugins.orientdb

import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Created by wtaie on 3/31/16.
 */

case class ExportOrientDbGraphArgs(graph: GraphReference,
                                   @ArgDoc("""The OrientDB URI which includes the database connection mode, database directory path and name .""") dbUri: String,
                                   @ArgDoc("""OrientDB username.""") userName: String,
                                   @ArgDoc("""OrientDB password.""") password: String,
                                   @ArgDoc("""batch size to be committed. Default is 1 or a positive value""") batchSize: Int = 1000) {

  require(graph != null, "graph is required")
  require(dbUri != null, "database Uri is required")
  require(userName != null, "userName is required")
  require(password != null, "password is required")
  require(batchSize > 0, "batch size should be a positive value")

}
