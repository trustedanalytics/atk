package org.trustedanalytics.atk.domain.datacatalog

import org.trustedanalytics.atk.domain.schema.FrameSchema
import spray.json.JsValue

case class CatalogServiceResponse(metadata: FrameSchema, data: List[JsValue])
