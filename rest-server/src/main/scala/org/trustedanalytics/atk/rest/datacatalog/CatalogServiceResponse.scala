package org.trustedanalytics.atk.rest.datacatalog

import org.trustedanalytics.atk.domain.schema.FrameSchema
import spray.json.JsValue

case class CatalogServiceResponse(metadata: FrameSchema, data: List[JsValue])
