package org.trustedanalytics.atk.plugins.pregel.core

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Variables for executing belief propagation.
 */
case class PluginArgs(graph: GraphReference,
                      @ArgDoc("""Name of the vertex property which contains the prior belief for the vertex.""") priorProperty: String,
                      @ArgDoc("""Name of the vertex property which will contain the posterior belief for each vertex.""") posteriorProperty: String,
                      @ArgDoc("""Name of the edge property that contains the edge weight for each edge.""") edgeWeightProperty: String = StringUtils.EMPTY,
                      @ArgDoc("""Belief propagation will terminate when the average change in posterior beliefs between supersteps is less than or equal to this threshold.""") convergenceThreshold: Double = 0d,
                      @ArgDoc("""The maximum number of supersteps that the algorithm will execute.The valid range is all positive int.""") maxIterations: Int = 20) {
  require(StringUtils.isNotBlank(priorProperty), "prior property name must not be empty")
  require(StringUtils.isNotBlank(posteriorProperty), "posterior property name must not be empty")
}

/**
 * The result object
 *
 * @param frameDictionaryOutput dictionary with vertex label type as key and vertex's frame as the value
 * @param time execution time
 */
case class Return(frameDictionaryOutput: Map[String, FrameEntity], time: Double)

