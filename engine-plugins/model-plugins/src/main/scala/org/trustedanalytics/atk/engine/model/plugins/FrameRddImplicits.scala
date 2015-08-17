package org.trustedanalytics.atk.engine.model.plugins

import org.apache.spark.frame.FrameRdd

/**
 * These implicits can be imported to add frame-related functions to model plugins
 */
object FrameRddImplicits {

  implicit def frameToFrameRddFunctions(frame: FrameRdd): FrameRddFunctions = {
    new FrameRddFunctions(frame)
  }
}
