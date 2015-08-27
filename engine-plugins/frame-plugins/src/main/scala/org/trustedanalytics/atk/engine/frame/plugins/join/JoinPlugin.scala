/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.engine.frame.plugins.join

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Schema }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.frame._
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

/** Json conversion for arguments and return value case classes */
object JoinJsonFormat {
  implicit val JoinFrameFormat = jsonFormat2(JoinFrameArgs)
  implicit val JoinArgsFormat = jsonFormat5(JoinArgs)
}

import JoinJsonFormat._

/**
 * Join two data frames (similar to SQL JOIN)
 */
@PluginDoc(oneLine = "Join two data frames (similar to SQL JOIN).",
  extended = "<TBD>")
class JoinPlugin extends SparkCommandPlugin[JoinArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/join"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def numberOfJobs(arguments: JoinArgs)(implicit invocation: Invocation): Int = 2

  /**
   * Join two data frames (similar to SQL JOIN)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments parameter contains information for the join operation (user supplied arguments to running this plugin)
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: JoinArgs)(implicit invocation: Invocation): FrameReference = {
    val leftFrame: SparkFrame = arguments.leftFrame.frame
    val rightFrame: SparkFrame = arguments.rightFrame.frame

    //first validate join columns are valid
    leftFrame.schema.validateColumnsExist(List(arguments.leftFrame.joinColumn))
    rightFrame.schema.validateColumnsExist(List(arguments.rightFrame.joinColumn))

    // Get estimated size of frame to determine whether to use a broadcast join
    val broadcastJoinThreshold = EngineConfig.broadcastJoinThreshold

    val joinedFrame = JoinRddFunctions.join(
      createRDDJoinParam(leftFrame, arguments.leftFrame.joinColumn, broadcastJoinThreshold),
      createRDDJoinParam(rightFrame, arguments.rightFrame.joinColumn, broadcastJoinThreshold),
      arguments.how,
      broadcastJoinThreshold,
      arguments.skewedJoinType
    )

    engine.frames.tryNewFrame(CreateEntityArgs(name = arguments.name,
      description = Some("created from join operation"))) {
      newFrame => newFrame.save(joinedFrame)
    }
  }

  //Create parameters for join
  private def createRDDJoinParam(frame: SparkFrame,
                                 joinColumn: String,
                                 broadcastJoinThreshold: Long): RddJoinParam = {
    //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.3.1+
    //https://issues.apache.org/jira/browse/SPARK-6465
    val genericRowFrame = new FrameRdd(frame.rdd.frameSchema, JoinRddFunctions.toGenericRowRdd(frame.rdd))

    val frameSize = if (broadcastJoinThreshold > 0) frame.sizeInBytes else None
    RddJoinParam(genericRowFrame, joinColumn, frameSize)
  }

}
