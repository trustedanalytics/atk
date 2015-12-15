package org.trustedanalytics.atk.plugins.pregel.core

import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Json conversion for arguments and return value case classes
 */
object JsonFormat {
  implicit val plugInFormat = jsonFormat9(PluginArgs)
  implicit val returnFormat = jsonFormat2(Return)
}
