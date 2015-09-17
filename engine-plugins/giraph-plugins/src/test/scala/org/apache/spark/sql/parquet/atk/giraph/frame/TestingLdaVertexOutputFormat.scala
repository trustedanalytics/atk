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

package org.apache.spark.sql.parquet.atk.giraph.frame

import org.trustedanalytics.atk.giraph.io.{ LdaEdgeData, LdaVertexData, LdaVertexId }
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.mapreduce.{ JobContext, OutputCommitter, TaskAttemptContext }
import org.apache.mahout.math.Vector

import scala.collection.mutable

import scala.collection.JavaConversions._

/**
 * Object holds global values for tests
 */
object TestingLdaOutputResults {

  val docResults = mutable.Map[String, Vector]()
  val wordResults = mutable.Map[String, Vector]()
  val topicGivenWord = mutable.Map[String, Vector]()
}

/**
 * OutputFormat for LDA testing
 */
class TestingLdaVertexOutputFormat extends VertexOutputFormat[LdaVertexId, LdaVertexData, LdaEdgeData] {

  override def createVertexWriter(context: TaskAttemptContext): VertexWriter[LdaVertexId, LdaVertexData, LdaEdgeData] = {
    new VertexWriter[LdaVertexId, LdaVertexData, LdaEdgeData] {

      override def initialize(context: TaskAttemptContext): Unit = {}

      override def writeVertex(vertex: Vertex[LdaVertexId, LdaVertexData, LdaEdgeData]): Unit = {
        if (vertex.getId.isDocument) {
          TestingLdaOutputResults.docResults += vertex.getValue.getDescription -> vertex.getValue.getLdaResult
        }
        else {
          TestingLdaOutputResults.wordResults += vertex.getValue.getDescription -> vertex.getValue.getLdaResult
          TestingLdaOutputResults.topicGivenWord += vertex.getValue.getDescription -> vertex.getValue.getTopicGivenWord
        }
      }

      override def close(context: TaskAttemptContext): Unit = {}
    }
  }

  override def checkOutputSpecs(context: JobContext): Unit = {}

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = new DummyOutputCommitter

}
