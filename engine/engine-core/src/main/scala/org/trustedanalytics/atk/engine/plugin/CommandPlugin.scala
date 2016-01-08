/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.engine.plugin

import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphReference }
import org.trustedanalytics.atk.domain.model.{ ModelEntity, ModelReference }
import org.trustedanalytics.atk.engine.frame.{ FrameImpl, Frame }
import org.trustedanalytics.atk.engine.graph.{ SparkGraphStorage, Graph, GraphImpl }
import org.trustedanalytics.atk.engine.model.{ ModelImpl, Model }
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware
import spray.json.{ JsObject, _ }

import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.util.control.NonFatal
import org.trustedanalytics.atk.engine.plugin.ApiMaturityTag.ApiMaturityTag

/**
 * Base trait for command plugins
 *
 * Plugin authors should implement the execute() method
 *
 * @tparam Arguments the type of the arguments that the plugin expects to receive from
 *                   the user
 * @tparam Return the type of the data that this plugin will return when invoked.
 */
abstract class CommandPlugin[Arguments <: Product: JsonFormat: ClassManifest: TypeTag, Return <: Product: JsonFormat: ClassManifest: TypeTag]
    extends EventLogging
    with ClassLoaderAware {

  // Implicit conversions for plugin authors
  implicit def frameRefToFrame(frame: FrameReference)(implicit invocation: Invocation): Frame = new FrameImpl(frame, engine.frames)
  implicit def frameEntityToFrame(frameEntity: FrameEntity)(implicit invocation: Invocation): Frame = frameRefToFrame(frameEntity.toReference)

  // TODO: get rid of cast
  implicit def graphRefToGraph(graph: GraphReference)(implicit invocation: Invocation): Graph = new GraphImpl(graph, engine.graphs.asInstanceOf[SparkGraphStorage])
  implicit def graphEntityToGraph(graphEntity: GraphEntity)(implicit invocation: Invocation): Graph = graphRefToGraph(graphEntity.toReference)

  implicit def modelRefToModel(model: ModelReference)(implicit invocation: Invocation): Model = new ModelImpl(model, engine.models)
  implicit def modelEntityToModel(modelEntity: ModelEntity)(implicit invocation: Invocation): Model = modelRefToModel(modelEntity.toReference)

  def engine(implicit invocation: Invocation) = invocation.asInstanceOf[CommandInvocation].engine

  val argumentManifest = implicitly[ClassManifest[Arguments]]
  val returnManifest = implicitly[ClassManifest[Return]]
  val argumentTag = implicitly[TypeTag[Arguments]]
  val returnTag = implicitly[TypeTag[Return]]
  val thisManifest = implicitly[ClassManifest[this.type]]
  val thisTag = implicitly[TypeTag[this.type]]

  /**
   * Runs setup, execute(), and cleanup()
   *
   * @return the results of calling the execute method
   */
  final def apply(simpleInvocation: Invocation, arguments: Arguments): Return = withPluginContext("apply")({
    require(simpleInvocation != null, "Invocation required")
    require(arguments != null, "Arguments required")

    //We call execute rather than letting plugin authors directly implement
    //apply so that if we ever need to put additional actions before or
    //after the plugin code, we can.
    withMyClassLoader {
      implicit val invocation = customizeInvocation(simpleInvocation, arguments)
      val result = try {
        debug("Invoking execute method with arguments:\n" + arguments)
        execute(arguments)(invocation)
      }
      finally {
        cleanup(invocation)
      }
      if (result == null) {
        throw new Exception(s"Plugin ${this.getClass.getName} returned null")
      }
      debug("Result was:\n" + result)
      result
    }

  })(simpleInvocation)

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class _BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   *
   * The key word 'new' has special meaning.  'model:lda/new' represents the constructor to LdaModel
   */
  def name: String

  /**
   * Optional Tag for the plugin API.
   *
   * This tag gets exposed in user documentation.
   */
  def apiMaturityTag: Option[ApiMaturityTag] = None

  /**
   * Number of jobs needs to be known to give a single progress bar
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @return number of jobs in this command
   */
  def numberOfJobs(arguments: Arguments)(implicit invocation: Invocation): Int = 1

  /**
   * Plugin authors should implement the execute() method.
   *
   * This is the main extension point for plugin authors.
   *
   * @param context information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  def execute(arguments: Arguments)(implicit context: Invocation): Return

  /**
   * Can be overridden by subclasses to provide a more specialized Invocation. Called before
   * calling the execute method.
   */
  protected def customizeInvocation(invocation: Invocation, arguments: Arguments): Invocation = {
    invocation
  }

  protected def cleanup(invocation: Invocation) = {}

  /**
   * Convert the given JsObject to an instance of the Argument type
   */
  def parseArguments(arguments: JsObject)(implicit invocation: Invocation): Arguments = withPluginContext("parseArguments") {
    arguments.convertTo[Arguments]
  }

  /**
   * Convert the given object to a JsObject
   */
  def serializeReturn(returnValue: Return)(implicit invocation: Invocation): JsObject = withPluginContext("serializeReturn") {
    returnValue.toJson.asJsObject
  }

  private def withPluginContext[T](context: String)(expr: => T)(implicit invocation: Invocation): T = {
    withContext(context) {
      EventContext.getCurrent.put("plugin_name", name)
      try {
        val caller = invocation.user.user
        EventContext.getCurrent.put("user", caller.username.getOrElse(caller.id.toString))
      }
      catch {
        case NonFatal(e) => EventContext.getCurrent.put("user-name-error", e.toString)
      }
      expr
    }(invocation.eventContext)
  }

  /**
   * plugins which execute python UDF will override this to true; by default this is false .
   * if true, additional files are shipped for udf execution during a yarn job
   */
  def executesPythonUdf = false

}
