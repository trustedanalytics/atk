
package org.trustedanalytics.atk.engine.metrics

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.metrics.MetricCollectionArgs
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation }

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

class MetricCollectionPlugin extends CommandPlugin[MetricCollectionArgs, UnitReturn] {

  override def name: String = "_admin:/_metrics"

  override def execute(arguments: MetricCollectionArgs)(implicit context: Invocation): UnitReturn = {
    MetricCollection.singleTimeExecution()
    new UnitReturn
  }
}
