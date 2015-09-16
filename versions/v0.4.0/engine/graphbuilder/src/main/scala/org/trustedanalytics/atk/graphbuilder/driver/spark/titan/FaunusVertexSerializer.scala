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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan

import java.io._

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.minlog.Log
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.commons.io.FileUtils

/**
 * Kryo serializer for Titan/Faunus vertices.
 */

class FaunusVertexSerializer extends Serializer[FaunusVertex] {

  /**
   * Reads bytes and returns a new Faunus vertex
   *
   * @param kryo  Kryo serializer
   * @param input Kryo input stream
   * @param inputClass Class of object to return
   * @return Faunux vertex
   */
  def read(kryo: Kryo, input: Input, inputClass: Class[FaunusVertex]): FaunusVertex = {
    val bytes = kryo.readObject(input, classOf[Array[Byte]])
    val inputStream = new DataInputStream(new ByteArrayInputStream(bytes))

    val faunusVertex = new FaunusVertex()
    faunusVertex.readFields(inputStream)
    faunusVertex
  }

  /**
   * Writes Faunus vertex to byte stream
   *
   * @param kryo  Kryo serializer
   * @param output Kryo output stream
   * @param faunusVertex Faunus vertex to serialize
   */
  def write(kryo: Kryo, output: Output, faunusVertex: FaunusVertex): Unit = {
    val outputStream = new ByteArrayOutputStream()
    faunusVertex.write(new DataOutputStream(outputStream))
    kryo.writeObject(output, outputStream.toByteArray)
  }
}
