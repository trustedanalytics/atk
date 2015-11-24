package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Json conversion for arguments and return value case classes */
object CollaborativeFilteringJsonFormat {

  implicit val cfTrainArgs = jsonFormat12(CollaborativeFilteringTrainArgs)
  implicit val cfFilterArgs = jsonFormat3(CollaborativeFilteringData)
  implicit val cfPredictArgs = jsonFormat3(CollaborativeFilteringPredictArgs)
}
