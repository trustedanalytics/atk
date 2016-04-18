package org.trustedanalytics.atk.plugins.orientdb

import org.trustedanalytics.atk.engine.plugin.ArgDoc
import scala.collection.immutable.Map

/**
 * Created by wtaie on 4/14/16.
 */

case class ExportOrientDbGraphReturn(@ArgDoc("""a tuple of two: the vertices class name and the corresponding number of exported Orient vertices.""") oVerticesStats: Map[String, Long],
                                     @ArgDoc("""a tuple of two: the edges class name and the corresponding number of exported Orient edges.""") oEdgeStats: Map[String, Long],
                                     @ArgDoc("""The created Orient database URI .""") dbUri: String)
