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

package org.apache.spark.api.python

import org.apache.spark.rdd.RDD
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import scala.reflect.ClassTag
import org.apache.spark.{ SparkException, SparkEnv, AccumulatorParam, Accumulator }
import org.apache.spark.util.Utils
import java.net.Socket
import java.io.{ BufferedOutputStream, DataOutputStream }

/**
 * Wrapper to enable access to private Spark class PythonRDD
 */
class EnginePythonRdd[T: ClassTag](
  parent: RDD[T],
  command: Array[Byte],
  envVars: JMap[String, String],
  pythonIncludes: JList[String],
  preservePartitioning: Boolean,
  pythonExec: String,
  broadcastVars: JList[Broadcast[AtkPythonBroadcast]],
  accumulator: Accumulator[JList[Array[Byte]]])
    extends PythonRDD(parent, command, envVars, pythonIncludes,
      preservePartitioning, pythonExec, broadcastVars.asInstanceOf[JList[Broadcast[PythonBroadcast]]], accumulator) {

}

class AtkPythonBroadcast extends PythonBroadcast("") {}

