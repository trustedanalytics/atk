package org.trustedanalytics.atk.domain.datacatalog

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol
import DomainJsonProtocol._

trait DataCatalogRestImplicits extends DefaultJsonProtocol {
  implicit val catalogServiceResponseFormat = jsonFormat2(CatalogServiceResponse)
  implicit val catalogmetadataFormat = jsonFormat9(CatalogMetadata)
  implicit val inputMetadataFormat = jsonFormat10(InputMetadataEntry)
}

object DataCatalogRestResponseJsonProtocol extends DataCatalogRestImplicits