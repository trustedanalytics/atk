package org.trustedanalytics.atk.engine.catalog

import org.trustedanalytics.atk.domain.catalog.Catalog
import org.trustedanalytics.atk.engine.catalog.datacatalog._
import org.trustedanalytics.atk.engine.catalog.framecatalog.FrameCatalog

object SimpleCatalogFactory {

  def getCatalogInstance(catalogType: String): Catalog = {
    catalogType.toLowerCase match {
      case "data" => AggregatedDataCatalog
      case "frames" => FrameCatalog
      // case "graphs" => GraphCatalog
      // case "models" => ModelCatalog
      case _ => throw new Exception("Unknown catalog")
    }
  }

}