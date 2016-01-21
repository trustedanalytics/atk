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

package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.engine.EngineExecutionContext
import org.trustedanalytics.atk.engine.plugin.Call
import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.scalatest.mock.MockitoSugar

class SparkGraphHBaseBackendTest extends WordSpec with Matchers with MockitoSugar {

  implicit val call = Call(null, EngineExecutionContext.global, "fakeClientId")
  //TODO: enable the test when TRIB-4413 is fixed
  //  "Not quietly deleting a table that does not exist" should {
  //    "throw an illegal argument exception" in {
  //
  //      val tableName = "table for none"
  //      val mockHBaseAdmin = mock[HBaseAdmin]
  //      when(mockHBaseAdmin.tableExists(tableName)).thenReturn(false)
  //
  //      val hbaseAdminFactory = mock[HBaseAdminFactory]
  //      when(hbaseAdminFactory.createHBaseAdmin()).thenReturn(mockHBaseAdmin)
  //
  //      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory)
  //
  //      an[IllegalArgumentException] should be thrownBy sparkGraphHBaseBackend.deleteUnderlyingTable(tableName, quiet = false)
  //
  //    }
  //  }

  //  "Quietly deleting a table that does exist" should {
  //    "cause table to be disabled and deleted" in {
  //      val userTableName = "mytable"
  //      val internalTableName = "iat_graph_mytable"
  //      val mockHBaseAdmin = mock[HBaseAdmin]
  //      when(mockHBaseAdmin.tableExists(internalTableName)).thenReturn(true)
  //      when(mockHBaseAdmin.isTableEnabled(internalTableName)).thenReturn(true)
  //
  //      val hbaseAdminFactory = mock[HBaseAdminFactory]
  //      when(hbaseAdminFactory.createHBaseAdmin()).thenReturn(mockHBaseAdmin)
  //
  //      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory)
  //
  //      sparkGraphHBaseBackend.deleteUnderlyingTable(userTableName, quiet = true)
  //
  //      verify(mockHBaseAdmin, times(1)).disableTable(internalTableName)
  //      verify(mockHBaseAdmin, times(1)).deleteTable(internalTableName)
  //    }
  //  }
}
