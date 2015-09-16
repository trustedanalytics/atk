package org.trustedanalytics.atk.engine.frame.plugins.experimental

import org.apache.spark.frame.FrameRdd
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.trustedanalytics.atk.domain.schema.DataTypes

/**
 * Class to encapsulate location info and UPC so that computation between 2 items can be computed later
 * @param upc UPC code for an item
 * @param rlx Reference Location X coordinate
 * @param rly Reference Location Y coordinate
 * @param clx Current Location X coordinate
 * @param cly Current Location Y coordinate
 */
case class LocationInfo(upc: String, rlx: Double, rly: Double, clx: Double, cly: Double)

object ComputeMisplacedScoreImpl {

  /**
   * Compute Misplaced score using broadcast join
   * @param rdd Frame containing item information
   * @param gravity Similarity value to account for gravitational pull between 2 items
   * @return New Frame with computed misplaced score for each item
   */

  def computeMisplacedScoreUsingBroadcastJoin(rdd: FrameRdd, gravity: Double): FrameRdd = {

    // Look up coordinate map and get x,y coordinates for location id
    val coordinateRdd = rdd.mapRows {
      case row =>
        val rlx = row.value("x_pos_ref").asInstanceOf[Long]
        val rly = row.value("y_pos_ref").asInstanceOf[Long]
        val clx = row.value("x_pos_cur").asInstanceOf[Long]
        val cly = row.value("y_pos_cur").asInstanceOf[Long]
        val upc = row.value("upc").asInstanceOf[String]
        (row.values(), LocationInfo(upc, rlx, rly, clx, cly))
    }

    val cbv = rdd.sparkContext.broadcast(coordinateRdd.collect())

    val result = coordinateRdd.map {
      case row =>
        var tension = 0.0
        for (i <- cbv.value)
          tension += computeTension(row._2, i._2, gravity)
        row._1.toArray :+ tension
    }

    FrameRdd.toFrameRdd(rdd.frameSchema.addColumn("misplaced_score", DataTypes.float64), result).sortByColumns(List(("misplaced_score", false)))
  }

  /**
   * Computes tension between 2 item given their location information
   * @param src Src location
   * @param dest Dest location
   * @param gravity Gravitational value for computing similarity
   * @return Tension between 2 connected items based on their previous and current location information
   */
  def computeTension(src: LocationInfo, dest: LocationInfo, gravity: Double): Double = {
    val x_pos_ref_L = src.rlx
    val y_pos_ref_L = src.rly
    val x_pos_cur_L = src.clx
    val y_pos_cur_L = src.cly
    val x_pos_ref_R = dest.rlx
    val y_pos_ref_R = dest.rly
    val x_pos_cur_R = dest.clx
    val y_pos_cur_R = dest.cly
    val upc_L = src.upc
    val upc_R = dest.upc
    val spring_length = Math.pow(Math.pow(x_pos_ref_L - x_pos_ref_R, 2) + Math.pow(y_pos_ref_L - y_pos_ref_R, 2), .5)
    val spring_flex = 1.0 / (Math.pow(spring_length, 2) + 1)
    val current_dist = Math.pow(Math.pow(x_pos_cur_L - x_pos_cur_R, 2) + Math.pow(y_pos_cur_L - y_pos_cur_R, 2), .5)
    val similarity = if (x_pos_cur_L == x_pos_cur_R && y_pos_cur_L == y_pos_cur_R && upc_L == upc_R)
      -gravity
    else 0
    val tension: Double = if (x_pos_ref_L != 9999 && x_pos_ref_R != 9999)
      spring_flex * Math.abs(current_dist - spring_length) - similarity
    else
      Math.min(0, 10000 - 10 * similarity)
    tension
  }

}
