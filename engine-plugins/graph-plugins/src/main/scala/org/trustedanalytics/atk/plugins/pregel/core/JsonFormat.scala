package org.trustedanalytics.atk.plugins.pregel.core

import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Json conversion for arguments and return value case classes
 */
object JsonFormat {

  implicit val BPFormat = jsonFormat6(PluginArgs)
  implicit val BPResultFormat = jsonFormat2(Return)
}
