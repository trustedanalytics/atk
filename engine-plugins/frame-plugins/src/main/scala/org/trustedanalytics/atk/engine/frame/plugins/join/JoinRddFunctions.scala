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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, GenericMutableRow }
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Schema }
import org.trustedanalytics.atk.engine.frame.plugins.join.JoinRddImplicits._

//implicit conversion for PairRDD

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object JoinRddFunctions extends Serializable {

  /**
   * Perform join operation
   *
   * Supports left-outer joins, right-outer-joins, outer-joins, and inner joins
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of one of the data frames is below threshold
   * @param how join method
   * @return Joined RDD
   */
  def join(left: RddJoinParam,
           right: RddJoinParam,
           how: String,
           broadcastJoinThreshold: Long = Long.MaxValue,
           skewedJoinType: Option[String] = None): FrameRdd = {

    val joinedRdd = how match {
      case "left" => leftOuterJoin(left, right, broadcastJoinThreshold, skewedJoinType)
      case "right" => rightOuterJoin(left, right, broadcastJoinThreshold, skewedJoinType)
      case "outer" => fullOuterJoin(left, right)
      case "inner" => innerJoin(left, right, broadcastJoinThreshold, skewedJoinType)
      case other => throw new IllegalArgumentException(s"Method $other not supported. only support left, right, outer and inner.")
    }

    createJoinedFrame(joinedRdd, left, right, how)
  }

  /**
   * Perform inner join
   *
   * Inner joins return all rows with matching keys in the first and second data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of one of the data frames is below threshold
   *
   * @return Joined RDD
   */
  def innerJoin(left: RddJoinParam,
                right: RddJoinParam,
                broadcastJoinThreshold: Long,
                skewedJoinType: Option[String] = None): RDD[Row] = {
    // Estimated size in bytes used to determine whether or not to use a broadcast join
    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    if (leftSizeInBytes < broadcastJoinThreshold || rightSizeInBytes < broadcastJoinThreshold) {
      left.innerBroadcastJoin(right, broadcastJoinThreshold)
    }
    else {
      val leftFrame = left.frame.toDataFrame
      val rightFrame = right.frame.toDataFrame
      val joinedFrame = leftFrame.join(
        rightFrame,
        leftFrame(left.joinColumn).equalTo(rightFrame(right.joinColumn))
      )
      //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.3.1+
      toGenericRowRdd(joinedFrame.rdd)
    }
  }

  /**
   * Perform full-outer join
   *
   * Full-outer joins return both matching, and non-matching rows in the first and second data frame.
   * Broadcast join is not supported.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   *
   * @return Joined RDD
   */
  def fullOuterJoin(left: RddJoinParam, right: RddJoinParam): RDD[Row] = {
    val leftFrame = left.frame.toDataFrame
    val rightFrame = right.frame.toDataFrame
    val joinedFrame = leftFrame.join(rightFrame,
      leftFrame(left.joinColumn).equalTo(rightFrame(right.joinColumn)),
      joinType = "fullouter"
    )
    //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.3.1+
    toGenericRowRdd(joinedFrame.rdd)
  }

  /**
   * Perform right-outer join
   *
   * Right-outer joins return all the rows in the second data-frame, and matching rows in the first data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of first data frame is below threshold
   *
   * @return Joined RDD
   */
  def rightOuterJoin(left: RddJoinParam,
                     right: RddJoinParam,
                     broadcastJoinThreshold: Long,
                     skewedJoinType: Option[String] = None): RDD[Row] = {
    // Estimated size in bytes used to determine whether or not to use a broadcast join
    val leftSizeInBytes = left.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    skewedJoinType match {
      case x if leftSizeInBytes < broadcastJoinThreshold => left.rightBroadcastJoin(right)
      case _ =>
        val leftFrame = left.frame.toDataFrame
        val rightFrame = right.frame.toDataFrame
        val joinedFrame = leftFrame.join(rightFrame,
          leftFrame(left.joinColumn).equalTo(rightFrame(right.joinColumn)),
          joinType = "right"
        )
        //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.3.1+
        toGenericRowRdd(joinedFrame.rdd)
    }
  }

  /**
   * Perform left-outer join
   *
   * Left-outer joins return all the rows in the first data-frame, and matching rows in the second data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of second data frame is below threshold
   *
   * @return Joined RDD
   */
  def leftOuterJoin(left: RddJoinParam,
                    right: RddJoinParam,
                    broadcastJoinThreshold: Long,
                    skewedJoinType: Option[String] = None): RDD[Row] = {
    val rightSizeInBytes = right.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    skewedJoinType match {
      case x if rightSizeInBytes < broadcastJoinThreshold => left.leftBroadcastJoin(right)
      case _ =>
        val leftFrame = left.frame.toDataFrame
        val rightFrame = right.frame.toDataFrame
        val joinedFrame = leftFrame.join(rightFrame,
          leftFrame(left.joinColumn).equalTo(rightFrame(right.joinColumn)),
          joinType = "left"
        )
        //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.3.1+
        toGenericRowRdd(joinedFrame.rdd)
    }
  }

  /**
   * Converts RDD of rows of GenericRowWithSchema to GenericRow
   *
   * Work-around for kyro serialization bug in GenericRowWithSchema
   *
   * @see https://issues.apache.org/jira/browse/SPARK-6465
   */
  def toGenericRowRdd(rdd: RDD[Row]): RDD[Row] = {
    //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.4
    rdd.map(row => new GenericRow(row.toSeq.toArray))
  }

  /**
   * Create joined frame
   *
   * The duplicate join column in the joined RDD is dropped in the joined frame.
   *
   * @param joinedRdd Joined RDD
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param how join method
   * @return Joined frame
   */
  def createJoinedFrame(joinedRdd: RDD[Row],
                        left: RddJoinParam,
                        right: RddJoinParam,
                        how: String): FrameRdd = {
    how match {
      case "outer" => {
        val mergedRdd = mergeJoinColumns(joinedRdd, left, right)
        dropRightJoinColumn(mergedRdd, left, right)
      }
      case "right" => {
        dropLeftJoinColumn(joinedRdd, left, right)
      }
      case _ => {
        dropRightJoinColumn(joinedRdd, left, right)
      }
    }
  }

  /**
   * Merge joined columns for full outer join
   *
   * Replaces null values in left join column with value in right join column
   *
   * @param joinedRdd Joined RDD
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @return Merged RDD
   */
  def mergeJoinColumns(joinedRdd: RDD[Row],
                       left: RddJoinParam,
                       right: RddJoinParam): RDD[Row] = {
    val leftSchema = left.frame.frameSchema
    val rightSchema = right.frame.frameSchema

    val leftJoinIndex = leftSchema.columnIndex(left.joinColumn)
    val rightJoinIndex = rightSchema.columnIndex(right.joinColumn) + rightSchema.columns.size

    joinedRdd.map(row => {
      val leftKey = row.get(leftJoinIndex)
      val rightKey = row.get(rightJoinIndex)
      val newLeftKey = if (leftKey == null) rightKey else leftKey

      val mutableRow = new GenericMutableRow(row.toSeq.toArray)
      mutableRow.update(leftJoinIndex, newLeftKey)
      mutableRow
    })
  }

  /**
   * Drop duplicate data in left join column
   *
   * Used for right-outer joins
   *
   * @param joinedRdd Joined RDD
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @return Joined frame
   */
  def dropLeftJoinColumn(joinedRdd: RDD[Row],
                         left: RddJoinParam,
                         right: RddJoinParam): FrameRdd = {
    val leftSchema = left.frame.frameSchema
    val rightSchema = right.frame.frameSchema

    // Get unique name for left join column to drop
    val oldSchema = FrameSchema(Schema.join(leftSchema.columns, rightSchema.columns))
    val dropColumnName = oldSchema.column(leftSchema.columnIndex(left.joinColumn)).name

    // Create new schema
    val newLeftSchema = leftSchema.renameColumn(left.joinColumn, dropColumnName)
    val newSchema = FrameSchema(Schema.join(newLeftSchema.columns, rightSchema.columns))

    new FrameRdd(newSchema, joinedRdd).dropColumns(List(dropColumnName))
  }

  /**
   * Drop duplicate data in right join column
   *
   * Used for inner, left-outer, and full-outer joins
   *
   * @param joinedRdd Joined RDD
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @return Joined frame
   */
  def dropRightJoinColumn(joinedRdd: RDD[Row],
                          left: RddJoinParam,
                          right: RddJoinParam): FrameRdd = {
    val leftSchema = left.frame.frameSchema
    val rightSchema = right.frame.frameSchema

    // Get unique name for right join column to drop
    val oldSchema = FrameSchema(Schema.join(leftSchema.columns, rightSchema.columns))
    val dropColumnName = oldSchema.column(leftSchema.columns.size + rightSchema.columnIndex(right.joinColumn)).name

    // Create new schema
    val newRightSchema = rightSchema.renameColumn(right.joinColumn, dropColumnName)
    val newSchema = FrameSchema(Schema.join(leftSchema.columns, newRightSchema.columns))

    new FrameRdd(newSchema, joinedRdd).dropColumns(List(dropColumnName))
  }
}
