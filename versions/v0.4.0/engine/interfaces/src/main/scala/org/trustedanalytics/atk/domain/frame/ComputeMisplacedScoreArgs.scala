package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

case class ComputeMisplacedScoreArgs(frame: FrameReference,
                                     @ArgDoc("Similarity measure for computing tension between 2 connected items") gravity: Double)
