package org.trustedanalytics.atk.plugins.orientdbimport


import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.engine.plugin.{Invocation, SparkCommandPlugin, PluginDoc}
import spray.json._

/** Json conversion for arguments and return value case classes */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object importOrientDbGraphJsonFormat {
  implicit val importOrientDbGraphArgsFormat = jsonFormat1(ImportOrientDbGraphArgs)
}
import org.trustedanalytics.atk.plugins.orientdbimport.importOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Imports a graph from OrientDB",
  extended =".",
  returns ="ATK graph.")
class ImportOrientDbGraphPlugin extends SparkCommandPlugin [ImportOrientDbGraphArgs, UnitReturn]{

  override def name: String = "graph:/import_orientdb"

  override def execute(arguments: ImportOrientDbGraphArgs)(implicit invocation: Invocation):UnitReturn = ???
}
