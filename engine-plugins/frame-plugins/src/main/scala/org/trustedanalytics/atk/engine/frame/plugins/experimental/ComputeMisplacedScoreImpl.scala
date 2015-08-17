package org.trustedanalytics.atk.engine.frame.plugins.experimental

import org.apache.spark.frame.FrameRdd
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.trustedanalytics.atk.domain.schema.DataTypes

case class LocationInfo(rlx: Double, rly: Double, clx: Double, cly: Double)

object ComputeMisplacedScoreImpl {

  def computeMisplacedScore(rdd: FrameRdd, bv: Broadcast[Map[String, (Double, Double)]]): FrameRdd = {

    val num_columns = rdd.frameSchema.columnNames.length

    val coordinateRdd = rdd.mapRows {
      case row =>
        val rl = row.value("rl").asInstanceOf[String]
        val cl = row.value("cl").asInstanceOf[String]
        val rlxy = bv.value.get(rl).get
        val clxy = bv.value.get(cl).get
        (row.values(), LocationInfo(rlxy._1, rlxy._2, clxy._1, clxy._2))
    }

    val allEdges = coordinateRdd.cartesian(coordinateRdd)

    val result = allEdges.map {
      case row =>
        val (src, dest) = (row._1._2, row._2._2)
        val x_pos_ref_L = src.rlx
        val y_pos_ref_L = src.rly
        val x_pos_cur_L = src.clx
        val y_pos_cur_L = src.cly
        val x_pos_ref_R = dest.rlx
        val y_pos_ref_R = dest.rly
        val x_pos_cur_R = dest.clx
        val y_pos_cur_R = dest.cly

        val spring_length = Math.pow(Math.pow(x_pos_ref_L - x_pos_ref_R, 2) + Math.pow(y_pos_ref_L - y_pos_ref_R, 2), .5)
        val spring_flex = 1.0 / (Math.pow(spring_length, 2) + 1)
        val current_dist = Math.pow(Math.pow(x_pos_cur_L - x_pos_cur_R, 2) + Math.pow(y_pos_cur_L - y_pos_cur_R, 2), .5)
        val tension = spring_flex * Math.abs(current_dist - spring_length)
        (row._1._1, tension)
    }.reduceByKey(_ + _)
      .map { case (k: Seq[Any], v: Double) => k.toArray :+ v }

    FrameRdd.toFrameRdd(rdd.frameSchema.addColumn("misplaced_score", DataTypes.float64), result)
  }

  def computeMisplacedScoreUsingBroadcastJoin(rdd: FrameRdd, bv: Broadcast[Map[String, (Double, Double)]]): FrameRdd = {

    val num_columns = rdd.frameSchema.columnNames.length

    val coordinateRdd = rdd.mapRows {
      case row =>
        val rl = row.value("rl").asInstanceOf[String]
        val cl = row.value("cl").asInstanceOf[String]
        println(s"locations: &$rl& #$cl#")
        val rlxy = bv.value.get(rl).get
        val clxy = bv.value.get(cl).get
        (row.values(), LocationInfo(rlxy._1, rlxy._2, clxy._1, clxy._2))
    }

    val cbv = rdd.sparkContext.broadcast(coordinateRdd.collect())

    val result = coordinateRdd.map {
      case row =>
        var tension = 0.0
        for (i <- cbv.value)
          tension += computeTension(row._2, i._2)
        row._1.toArray :+ tension
    }

    FrameRdd.toFrameRdd(rdd.frameSchema.addColumn("misplaced_score", DataTypes.float64), result).sortByColumns(List(("misplaced_score", false)))
  }

  def computeTension(src: LocationInfo, dest: LocationInfo): Double = {
    val x_pos_ref_L = src.rlx
    val y_pos_ref_L = src.rly
    val x_pos_cur_L = src.clx
    val y_pos_cur_L = src.cly
    val x_pos_ref_R = dest.rlx
    val y_pos_ref_R = dest.rly
    val x_pos_cur_R = dest.clx
    val y_pos_cur_R = dest.cly
    val spring_length = Math.pow(Math.pow(x_pos_ref_L - x_pos_ref_R, 2) + Math.pow(y_pos_ref_L - y_pos_ref_R, 2), .5)
    val spring_flex = 1.0 / (Math.pow(spring_length, 2) + 1)
    val current_dist = Math.pow(Math.pow(x_pos_cur_L - x_pos_cur_R, 2) + Math.pow(y_pos_cur_L - y_pos_cur_R, 2), .5)
    spring_flex * Math.abs(current_dist - spring_length)
  }

}
