package org.trustedanalytics.atk.plugins.orientdb
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
  * Created by wtaie on 3/31/16.
  */
case class ExportOrientDbGraphArgs(graph: GraphReference,
                                   @ArgDoc("""The OrientDB URL which includes the database connection mode, database directory path and name .""") dbUrl: String,
                                   @ArgDoc("""OrientDB username.Default is admin""") userName: String,
                                   @ArgDoc("""OrientDB password. Default is admin""") password: String,
                                   @ArgDoc("""batch size to be committed. Default is 1 or a positive value""") batchSize: Int = 1000) {
  require(graph != null, "graph is required")
  require(dbUrl!= null, "url is required")
  require(userName != null, "userName is required")
  require(password != null, "password is required")
  require(batchSize > 0, "batch size should be a positive value")

}
