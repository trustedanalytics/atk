package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

case class ComputeMisplacedScoreArgs(frame: FrameReference,
                                     @ArgDoc("""<TBD>""") locationFrame: FrameReference)
