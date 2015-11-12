package org.trustedanalytics.atk.domain.util

import spray.json._

object DataToJson {

  /**
   * Convert an Iterable of Any to a List of JsValue. Required due to how spray-json handles AnyVals
   * @param data iterable to return in response
   * @return JSON friendly version of data
   */
  def apply(data: Iterable[Array[Any]]): List[JsValue] = {
    import org.trustedanalytics.atk.domain.DomainJsonProtocol._
    data.map(row => row.map {
      case null => JsNull
      case a => a.toJson
    }.toJson).toList
  }

}
