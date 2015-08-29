package org.trustedanalytics.atk.engine.frame.plugins.join

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Functions for joining pair RDDs using broadcast variables
 */
class BroadcastJoinRddFunctions(self: RddJoinParam) extends Logging with Serializable {

  /**
   * Perform left outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of left-outer join
   */
  def leftBroadcastJoin(other: RddJoinParam): RDD[Row] = {
    val rightBroadcastVariable = JoinBroadcastVariable(other)
    lazy val rightNullRow: Row = new GenericRow(other.frame.numColumns)

    self.frame.flatMapRows(left => {
      val leftKey = left.value(self.joinColumn)
      rightBroadcastVariable.get(leftKey) match {
        case Some(rightRowSet) => for (rightRow <- rightRowSet) yield Row.merge(left.row, rightRow)
        case _ => List(Row.merge(left.row, rightNullRow.copy()))
      }
    })
  }

  /**
   * Right outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of right-outer join
   */
  def rightBroadcastJoin(other: RddJoinParam): RDD[Row] = {
    val leftBroadcastVariable = JoinBroadcastVariable(self)
    lazy val leftNullRow: Row = new GenericRow(self.frame.numColumns)

    other.frame.flatMapRows(right => {
      val rightKey = right.value(other.joinColumn)
      leftBroadcastVariable.get(rightKey) match {
        case Some(leftRowSet) => for (leftRow <- leftRowSet) yield Row.merge(leftRow, right.row)
        case _ => List(Row.merge(leftNullRow.copy(), right.row))
      }
    })
  }

  /**
   * Inner-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of inner-outer join
   */
  def innerBroadcastJoin(other: RddJoinParam, broadcastJoinThreshold: Long): RDD[Row] = {
    val leftSizeInBytes = self.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = other.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val innerJoinedRDD = if (rightSizeInBytes <= broadcastJoinThreshold) {
      val rightBroadcastVariable = JoinBroadcastVariable(other)
      self.frame.flatMapRows(left => {
        val leftKey = left.value(self.joinColumn)
        rightBroadcastVariable.get(leftKey) match {
          case Some(rightRowSet) =>
            for (rightRow <- rightRowSet) yield Row.merge(left.row, rightRow)
          case _ => Set.empty[Row]
        }
      })
    }
    else if (leftSizeInBytes <= broadcastJoinThreshold) {
      val leftBroadcastVariable = JoinBroadcastVariable(self)
      other.frame.flatMapRows(rightRow => {
        val rightKey = rightRow.value(other.joinColumn)
        leftBroadcastVariable.get(rightKey) match {
          case Some(leftRowSet) =>
            for (leftRow <- leftRowSet) yield Row.merge(leftRow, rightRow.row)
          case _ => Set.empty[Row]
        }
      })
    }
    else throw new IllegalArgumentException(s"Frame size exceeds broadcast-join-threshold: $broadcastJoinThreshold.")
    innerJoinedRDD
  }
}
