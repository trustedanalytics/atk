package org.trustedanalytics.atk.engine.catalog.datacatalog

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import org.trustedanalytics.atk.domain.catalog.{ CatalogResponse, GenericCatalogResponse, DataCatalog }
import org.trustedanalytics.atk.engine.{ Engine, EngineConfig }
import org.trustedanalytics.atk.engine.plugin.Invocation

object HdfsDataCatalog extends DataCatalog {
  override val name: String = "HDFS"
  override def list(engine: Engine)(implicit invocation: Invocation): List[CatalogResponse] = {
    val data_root = EngineConfig.fsRoot

    val fs = FileSystem.get(new Configuration())
    val files = fs.listStatus(new Path(data_root)).filterNot(_.getPath.getName.startsWith("."))

    val metadata = List("path", "isDirectory", "modification_time", "access_time", "size")
    val data = files.map(file => List(
      file.getPath,
      file.isDirectory,
      file.getModificationTime,
      file.getAccessTime,
      file.getLen).map(_.toString)
    ).toList

    List(GenericCatalogResponse(name, metadata, data))
  }
}
