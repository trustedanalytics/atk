package org.trustedanalytics.atk.engine.frame.plugins.join

/**
 * These implicits can be imported to add join related functions to RDD's
 */
object JoinRddImplicits {

  /**
   * Functions for joining RDDs using broadcast variables
   */
  implicit def joinRddToBroadcastJoinRddFunctions(joinParam: RddJoinParam): BroadcastJoinRddFunctions =
    new BroadcastJoinRddFunctions(joinParam)

}
