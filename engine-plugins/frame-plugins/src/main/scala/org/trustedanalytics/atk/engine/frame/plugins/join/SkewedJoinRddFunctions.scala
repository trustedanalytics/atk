package org.trustedanalytics.atk.engine.frame.plugins.join

import org.trustedanalytics.atk.engine.frame.plugins.join.JoinRddImplicits._
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.util.ReservoirSampler

import scala.util.hashing._

class SkewedJoinRddFunctions(self: RddJoinParam) extends Logging with Serializable {
  /*
  /**
   * Perform skewed left outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of left-outer join
   */
  def leftSkewedBroadcastJoin(other: RddJoinParam): RDD[(Any, (Row, Option[Row]))] = {
    val sparkContext = self.frame.rdd.sparkContext
    val leftSuperNodes = SupernodeFinder(self.frame).findSuperNodes()

    val keyCountsBroadcastVar = sparkContext.broadcast(leftSuperNodes)
    val (leftSuperNode, leftRegularNode) = splitRddBySuperNodes(self, keyCountsBroadcastVar)
    val (rightSuperNode, rightRegularNode) = splitRddBySuperNodes(other, keyCountsBroadcastVar)

    val superNodeJoinedRdd = leftSuperNode.leftBroadcastJoin(rightSuperNode)
    val regularNodeJoinedRdd = leftRegularNode.frame.leftOuterJoin(rightRegularNode.frame)

    regularNodeJoinedRdd.union(superNodeJoinedRdd)
  }

  /**
   * Perform skewed inner join
   *
   * Inner joins return all rows with matching keys in the first and second data frame.
   *
   * @param other join parameter for second data frame
   *
   * @return Joined RDD
   */
  def leftSkewedHashJoin(other: RddJoinParam): RDD[(Any, (Row, Option[Row]))] = {
    //TODO: Fix garbage collection issues in shuffle (find way to determine how to split keys)
    val sc = self.frame.sparkContext
    val leftSuperNodes = SupernodeFinder(self.frame).findSuperNodes()
    val rightSuperNodes = SupernodeFinder(other.frame).findSuperNodes()

    val sampledKeyCounts = mergeSampledKeys(leftSuperNodes, rightSuperNodes)
    val keyCountsBroadcastVar = sc.broadcast(sampledKeyCounts)
    val repartitionedLeftRdd = splitKeys(self.frame, keyCountsBroadcastVar, isRightRdd = false)

    val replicatedRightRdd = replicateKeys(other.frame, keyCountsBroadcastVar, isRightRdd = true)
    repartitionedLeftRdd.leftOuterJoin(replicatedRightRdd).map {
      case ((key, i), ((leftValues, lr), rightValuesPair)) =>
        rightValuesPair match {
          case r: Some[(Row, Int)] => (key, (leftValues, Some(r.get._1)))
          case None => (key, (leftValues, None))
        }
    }
  }

  /**
   * Perform skewed right outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of right-outer join
   */
  def rightSkewedBroadcastJoin(other: RddJoinParam): RDD[(Any, (Option[Row], Row))] = {
    val sc = self.frame.sparkContext
    val rightSuperNodes = SupernodeFinder(other.frame).findSuperNodes()

    val keyCountsBroadcastVar = sc.broadcast(rightSuperNodes)
    val (leftSuperNode, leftRegularNode) = splitRddBySuperNodes(self, keyCountsBroadcastVar)
    val (rightSuperNode, rightRegularNode) = splitRddBySuperNodes(other, keyCountsBroadcastVar)

    val superNodeJoinedRdd = leftSuperNode.rightBroadcastJoin(rightSuperNode)
    val regularNodeJoinedRdd = leftRegularNode.frame.rightOuterJoin(rightRegularNode.frame)

    regularNodeJoinedRdd.union(superNodeJoinedRdd)
  }

  /**
   * Perform skewed inner-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of inner join
   */
  def innerSkewedBroadcastJoin(other: RddJoinParam): RDD[(Any, (Row, Row))] = {
    val sc = self.frame.sparkContext

    val leftSuperNodes = SupernodeFinder(self.frame).findSuperNodes()
    val rightSuperNodes = SupernodeFinder(other.frame).findSuperNodes()
    val allSuperNodes = leftSuperNodes ++ rightSuperNodes

    val keyCountsBroadcastVar = sc.broadcast(allSuperNodes)
    val (leftSuperNode, leftRegularNode) = splitRddBySuperNodes(self, keyCountsBroadcastVar)
    val (rightSuperNode, rightRegularNode) = splitRddBySuperNodes(other, keyCountsBroadcastVar)

    val superNodeJoinedRdd = leftSuperNode.innerBroadcastJoin(rightSuperNode, Long.MaxValue)
    val regularNodeJoinedRdd = leftRegularNode.frame.join(rightRegularNode.frame)

    regularNodeJoinedRdd.union(superNodeJoinedRdd)
  }

  /**
   *
   * @param joinParam
   * @param superNodeCounts
   * @return
   */
  private def splitRddBySuperNodes(joinParam: RddJoinParam,
                                   superNodeCounts: Broadcast[Map[Any, KeyFrequency]]): (RddJoinParam, RddJoinParam) = {
    val superNodeRdd = joinParam.frame.filter { case (key, value) => superNodeCounts.value.contains(key) }
    val regularNodeRdd = joinParam.frame.filter { case (key, value) => !superNodeCounts.value.contains(key) }
    (RddJoinParam(superNodeRdd, joinParam.columnCount), RddJoinParam(regularNodeRdd, joinParam.columnCount))
  }

  def mergeSampledKeys[K](leftMap: Map[K, KeyFrequency],
                          rightMap: Map[K, KeyFrequency]): Map[K, (KeyFrequency, KeyFrequency)] = {
    val sampledKeyCounts = for {
      k <- leftMap.keySet ++ rightMap.keySet
      mergedCount = k -> (leftMap.getOrElse(k, KeyFrequency(0, 0)), rightMap.getOrElse(k, KeyFrequency(0, 0)))
    } yield mergedCount
    sampledKeyCounts.toMap
  }

  private def splitKeys[K, V](rdd: RDD[(K, V)],
                              sampledKeyCounts: Broadcast[Map[K, (KeyFrequency, KeyFrequency)]],
                              isRightRdd: Boolean,
                              seed: Long = 0): RDD[((K, Int), (V, Int))] = {
    val shift = rdd.id

    rdd.mapPartitionsWithIndex {
      case (idx, iter) =>
        val seed = byteswap32(idx ^ (shift << 16))
        val rand = ReservoirSampler.createXORShiftRandom(seed)

        iter.map {
          case (key, value) =>
            val (leftSuperNodes, rightSuperNodes) = sampledKeyCounts.value.getOrElse(key, (KeyFrequency(), KeyFrequency()))
            val numSplits = Math.max(if (isRightRdd) {
              rightSuperNodes.estimatedNumPartitions
            }
            else {
              leftSuperNodes.estimatedNumPartitions
            }, 1)
            val r = if (numSplits > 1) {
              rand.nextInt(numSplits)
            }
            else {
              0
            }
            ((key, r), (value, 1))
        }
    }
  }

  private def replicateKeys[K, V](rdd: RDD[(K, V)],
                                  sampledKeyCounts: Broadcast[Map[K, (KeyFrequency, KeyFrequency)]],
                                  isRightRdd: Boolean,
                                  seed: Long = 0): RDD[((K, Int), (V, Int))] = {
    rdd.flatMap {
      case (key, value) =>
        val (leftSupernodes, rightSupernodes) = sampledKeyCounts.value.getOrElse(key, (KeyFrequency(), KeyFrequency()))
        val numSplits = Math.max(if (isRightRdd) {
          leftSupernodes.estimatedNumPartitions
        }
        else {
          rightSupernodes.estimatedNumPartitions
        }, 1)
        for (i <- 0 until numSplits) yield ((key, i), (value, numSplits.toInt))
    }
  }*/
}
