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

package org.trustedanalytics.atk.domain

/**
 * Storage formats supported by the system
 */
object StorageFormats {

  val CassandraTitan = "cassandra/titan"
  val HBaseTitan = "hbase/titan"
  val SeamlessGraph = "atk/frame"
  val FileParquet = "file/parquet"
  val FileSequence = "file/sequence"

  private val graphFormats = Set(SeamlessGraph, CassandraTitan, HBaseTitan)
  private val frameFormats = Set(FileSequence, FileParquet)

  def validateGraphFormat(format: String): Unit = {
    if (!graphFormats.contains(format)) {
      throw new IllegalArgumentException(s"Unsupported graph storage format $format, please choose from " + graphFormats.mkString(", "))
    }
  }

  def validateFrameFormat(format: String): Unit = {
    if (!frameFormats.contains(format)) {
      throw new IllegalArgumentException(s"Unsupported frame storage format $format")
    }
  }

  def isSeamlessGraph(format: String): Boolean = {
    SeamlessGraph == format
  }

}
