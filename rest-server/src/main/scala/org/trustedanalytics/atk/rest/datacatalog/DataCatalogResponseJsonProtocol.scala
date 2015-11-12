package org.trustedanalytics.atk.rest.datacatalog

import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

object DataCatalogResponseJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val indexedMetadataEntryFormat = jsonFormat12(IndexedMetadataEntryWithID)
  implicit val datacatalogResponseFormat = jsonFormat4(DataCatalogResponse)
  implicit val catalogServiceResponseFormat = jsonFormat2(CatalogServiceResponse)
}
