
package org.trustedanalytics.atk.engine.kamon

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.kamon.KamonCollectionArgs
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation }

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

class KamonCollectPlugin extends CommandPlugin[KamonCollectionArgs, UnitReturn] {

  override def name: String = "_admin:/_kamon_metrics"

  override def execute(arguments: KamonCollectionArgs)(implicit context: Invocation): UnitReturn = {
    KamonCollect.singleTimeExecution()
    new UnitReturn
  }
}
