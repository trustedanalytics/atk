package org.trustedanalytics.atk.engine.catalog.framecatalog

import org.trustedanalytics.atk.domain.catalog.{ DataCatalog, GenericCatalogResponse, CatalogResponse }
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation

object FrameCatalog extends DataCatalog {
  override val name: String = "Frames"
  def list(engine: Engine)(implicit invocation: Invocation): List[CatalogResponse] = {
    val frames = engine.frames.getFrames()
    val metadata = List("name", "description", "schema", "createdOn", "modifiedOn", "storageLocation", "rowCount", "lastReadDate")
    val data = for (f <- frames)
      yield List(f.name.getOrElse(""), f.description, f.schema.columnNamesAsString, f.createdOn, f.modifiedOn, f.storageLocation, f.rowCount.getOrElse(0), f.lastReadDate).map(_.toString)
    List(GenericCatalogResponse(name, metadata, data.toList))
  }
}
