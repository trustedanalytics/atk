package org.trustedanalytics.atk.plugins.pregel.core

/**
 * Companion object holds the default values.
 */
object DefaultValues {
  val edgeWeightDefault = 1.0d
  val powerDefault = 0d
  val smoothingDefault = 1.0d
  val priorDefault = 1d
  val deltaDefault = 0d
  val separatorDefault: Array[Char] = Array(' ', ',', '\t')
}

object Initializers extends Serializable {

  /**
   * Default message set.
   * @return an empty map
   */
  def defaultMsgSet(): Map[Long, Vector[Double]] = {
    Map().asInstanceOf[Map[Long, Vector[Double]]]
  }

  /**
   * Default edge set.
   * @return an empty map
   */
  def defaultEdgeSet(): Set[(Long, Long)] = {
    Set().asInstanceOf[Set[(Long, Long)]]
  }

  /**
   * Default vertex set.
   * @return an empty map
   */
  def defaultVertexSet(): Set[Long] = {
    Set().asInstanceOf[Set[Long]]
  }
}

