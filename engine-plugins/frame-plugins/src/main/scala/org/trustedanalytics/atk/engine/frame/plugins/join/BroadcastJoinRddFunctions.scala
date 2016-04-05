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

package org.trustedanalytics.atk.engine.frame.plugins.join

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Functions for joining pair RDDs using broadcast variables
 */
class BroadcastJoinRddFunctions(self: RddJoinParam) extends Logging with Serializable {

  /**
   * Perform left outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
    * @return key-value RDD whose values are results of left-outer join
   */
  def leftBroadcastJoin(other: RddJoinParam): RDD[Row] = {
    val rightBroadcastVariable = JoinBroadcastVariable(other)
    lazy val rightNullRow: Row = new GenericRow(other.frame.numColumns)

    val rowWrapper = new RowWrapper(other.frame.frameSchema)
    val rightColsToKeep = other.frame.frameSchema.dropColumns(other.joinColumns.toList).columnNames
    self.frame.flatMapRows(left => {

      val leftKeys = left.values(self.joinColumns.toList)
      rightBroadcastVariable.get(leftKeys) match {
        case Some(rightRowSet) => for (rightRow <- rightRowSet) yield Row.merge(left.row, new GenericRow(rowWrapper(rightRow).values(rightColsToKeep).toArray))
        case _ => List(Row.merge(left.row, rightNullRow.copy()))
      }

    })
  }

  /**
   * Right outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
    * @return key-value RDD whose values are results of right-outer join
   */
  def rightBroadcastJoin(other: RddJoinParam): RDD[Row] = {
    val leftBroadcastVariable = JoinBroadcastVariable(self)
    lazy val leftNullRow: Row = new GenericRow(self.frame.numColumns)
    val rowWrapper = new RowWrapper(other.frame.frameSchema)

    other.frame.flatMapRows(right => {
      //      val rightKey = right.value(other.joinColumn)
      val leftColsToKeep = self.frame.frameSchema.dropColumns(self.joinColumns.toList).columnNames
      val rightKeys = right.values(other.joinColumns.toList)
      leftBroadcastVariable.get(rightKeys) match {
        case Some(leftRowSet) => for (leftRow <- leftRowSet) yield Row.merge(new GenericRow(rowWrapper(leftRow).values(leftColsToKeep).toArray), right.row)
        case _ => List(Row.merge(leftNullRow.copy(), right.row))
      }
    })
  }

  /**
   * Inner-join using a broadcast variable
   *
   * @param other join parameter for second data frame
    * @return key-value RDD whose values are results of inner-outer join
   */
  def innerBroadcastJoin(other: RddJoinParam, broadcastJoinThreshold: Long): RDD[Row] = {
    val leftSizeInBytes = self.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = other.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val rowWrapper = new RowWrapper(other.frame.frameSchema)
    val innerJoinedRDD = if (rightSizeInBytes <= broadcastJoinThreshold) {
      val rightBroadcastVariable = JoinBroadcastVariable(other)

      val rightColsToKeep = other.frame.frameSchema.dropColumns(other.joinColumns.toList).columnNames
      self.frame.flatMapRows(left => {
        //        val leftKey = left.value(self.joinColumn)
        val leftKeys = left.values(self.joinColumns.toList)
        rightBroadcastVariable.get(leftKeys) match {
          case Some(rightRowSet) =>
            for (rightRow <- rightRowSet) yield Row.merge(left.row, new GenericRow(rowWrapper(rightRow).values(rightColsToKeep).toArray))
          case _ => Set.empty[Row]
        }
      })
    }
    else if (leftSizeInBytes <= broadcastJoinThreshold) {
      val leftBroadcastVariable = JoinBroadcastVariable(self)
      other.frame.flatMapRows(rightRow => {
        //        val rightKey = rightRow.value(other.joinColumn)
        val leftColsToKeep = self.frame.frameSchema.dropColumns(self.joinColumns.toList).columnNames
        val rightKeys = rightRow.values(other.joinColumns.toList)
        leftBroadcastVariable.get(rightKeys) match {
          case Some(leftRowSet) =>
            for (leftRow <- leftRowSet) yield Row.merge(new GenericRow(rowWrapper(leftRow).values(leftColsToKeep).toArray), rightRow.row)
          case _ => Set.empty[Row]
        }
      })
    }
    else throw new IllegalArgumentException(s"Frame size exceeds broadcast-join-threshold: $broadcastJoinThreshold.")
    innerJoinedRDD
  }
}
