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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, GenericMutableRow }
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Schema }
import org.trustedanalytics.atk.engine.frame.plugins.join.JoinRddImplicits._

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
        left.joinColumns
      )
      joinedFrame.rdd
    }
  }

  /**
   * expression maker helps for generating conditions to check when join invoked with composite keys
   *
   * @param leftFrame left data frame
   * @param rightFrame rigth data frame
   * @param leftJoinCols list of left frame column names used in join
   * @param rightJoinCols list of right frame column name used in join
   * @return
   */
  def expressionMaker(leftFrame: DataFrame, rightFrame: DataFrame, leftJoinCols: Seq[String], rightJoinCols: Seq[String]): Column = {
    val columnsTuple = leftJoinCols.zip(rightJoinCols)

    def makeExpression(leftCol: String, rightCol: String): Column = {
      leftFrame(leftCol).equalTo(rightFrame(rightCol))
    }
    val expression = columnsTuple.map { case (lc, rc) => makeExpression(lc, rc) }.reduce(_ && _)
    expression
  }

  /**
   * Perform full-outer join
   *
   * Full-outer joins return both matching, and non-matching rows in the first and second data frame.
   * Broadcast join is not supported.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @return Joined RDD
   */
  def fullOuterJoin(left: RddJoinParam, right: RddJoinParam): RDD[Row] = {
    val leftFrame = left.frame.toDataFrame
    val rightFrame = right.frame.toDataFrame
    val expression = expressionMaker(leftFrame, rightFrame, left.joinColumns, right.joinColumns)
    val joinedFrame = leftFrame.join(rightFrame,
      expression,
      joinType = "fullouter"
    )
    joinedFrame.rdd
  }

  /**
   * Perform right-outer join
   *
   * Right-outer joins return all the rows in the second data-frame, and matching rows in the first data frame.
   *
   * @param left join parameter for first data frame
   * @param right join parameter for second data frame
   * @param broadcastJoinThreshold use broadcast variable for join if size of first data frame is below threshold
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
        val expression = expressionMaker(leftFrame, rightFrame, left.joinColumns, right.joinColumns)
        val joinedFrame = leftFrame.join(rightFrame,
          expression,
          joinType = "right"
        )
        joinedFrame.rdd
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
        val expression = expressionMaker(leftFrame, rightFrame, left.joinColumns, right.joinColumns)
        val joinedFrame = leftFrame.join(rightFrame,
          expression,
          joinType = "left"
        )

        joinedFrame.rdd
    }
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
        dropRightJoinColumn(mergedRdd, left, right, how)
      }
      case "right" => {
        dropLeftJoinColumn(joinedRdd, left, right)
      }
      case _ => {
        dropRightJoinColumn(joinedRdd, left, right, how)
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
    val leftJoinIndices = leftSchema.columnIndices(left.joinColumns)
    val rightJoinIndices = rightSchema.columnIndices(right.joinColumns).map(rightindex => rightindex + leftSchema.columns.size)

    joinedRdd.map(row => {
      val rowArray = row.toSeq.toArray
      leftJoinIndices.zip(rightJoinIndices).map {
        case (leftIndex, rightIndex) => {
          if (row.get(leftIndex) == null) {
            rowArray(leftIndex) = row.get(rightIndex)
          }
        }
      }
      new GenericRow(rowArray)
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
    val newSchema = FrameSchema(Schema.join(leftSchema.columns, rightSchema.columns))
    val frameRdd = new FrameRdd(newSchema, joinedRdd)
    val leftColNames = left.joinColumns.map(col => col + "_L")
    frameRdd.dropColumns(leftColNames.toList)
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
                          right: RddJoinParam,
                          how: String): FrameRdd = {

    val leftSchema = left.frame.frameSchema
    val rightSchema = right.frame.frameSchema

    // Create new schema
    if (how == "inner") {
      val newRightSchema = rightSchema.dropColumns(right.joinColumns.toList)
      val newSchema = FrameSchema(Schema.join(leftSchema.columns, newRightSchema.columns))
      new FrameRdd(newSchema, joinedRdd)
    }
    else {
      val newSchema = FrameSchema(Schema.join(leftSchema.columns, rightSchema.columns))
      val frameRdd = new FrameRdd(newSchema, joinedRdd)
      val rightColNames = right.joinColumns.map(col => col + "_R")
      frameRdd.dropColumns(rightColNames.toList)
    }
  }
}
