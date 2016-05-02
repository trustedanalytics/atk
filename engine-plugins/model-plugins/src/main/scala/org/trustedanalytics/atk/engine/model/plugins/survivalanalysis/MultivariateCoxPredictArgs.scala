package org.trustedanalytics.atk.engine.model.plugins.survivalanalysis

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class MultivariateCoxPredictArgs(model: ModelReference,
                                      frame: FrameReference,
                                      featureColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
}
