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

/* This script is to generate a bipartite graph using spark-shell in distributed mode */
/* Invoke this script as spark-shell -i bipartite-graph-generator.scala -Xnojline */
Thread.sleep(5000)
println("""
 ******************************************************************************
 * Run this sript in Spark Shell to create a bipartite graph
 * LEFT_SIDE_MAX -> Max Number of nodes in the left side of the graph e.g. 1000
 * RIGHT_SIDE_MAX -> Max Number of nodes in the right side of the graph e.g. 10000")
 * GRAPH_CONNECTEDNESS_DENSITY -> Completeness of the Graph. Range is 0 to 1. 0 means no nodes are connected and
                                  1 means a completely connected bipartite graph.
                                  Use a sparse value for large graphs e.g. 0.15
 * HDFS_OUTPUT_LOCATION -> Output location to store the bipartite graph in HDFS
 ******************************************************************************
""")

println("Enter Max Number of nodes in the left side of the graph e.g. 1000")
val LEFT_SIDE_MAX = readInt()
println("Enter Max Number of nodes in the right side of the graph e.g. 10000")
val RIGHT_SIDE_MAX = readInt()
println("Enter density of the graph between 0-1 e.g. 0.15")
val GRAPH_CONNECTEDNESS_DENSITY = readDouble()
println("Enter HDFS Output location e.g. hdfs://master/user/atkuser/bipartite_graph")
val HDFS_OUTPUT_LOCATION = readLine()

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import scala.util.Random

val left_nodes = new Range(1, LEFT_SIDE_MAX, 1)
val right_nodes = new Range(-1, -1 * RIGHT_SIDE_MAX, -1)

val distributed_left_nodes = sc.parallelize(left_nodes)
val distributed_right_nodes = sc.parallelize(right_nodes)

val connected_left_nodes = distributed_left_nodes.map((Random.nextInt().abs % 100 * GRAPH_CONNECTEDNESS_DENSITY, _))
val connected_right_nodes = distributed_right_nodes.map((Random.nextInt().abs % 100 * GRAPH_CONNECTEDNESS_DENSITY, _))

val graph = connected_left_nodes.join(connected_right_nodes).map(_._2)

val rated_graph = graph.map(row => s"${row._1},${row._2},${Random.nextInt().abs % 10}")
rated_graph.saveAsTextFile(HDFS_OUTPUT_LOCATION)

println("******************************************************")
println(s"Output files have been saved to $HDFS_OUTPUT_LOCATION")
println("Do you wish to merge the output files to a single file on HDFS and delete output directory? Input file location, otherwise just hit Enter")
println("e.g. hdfs://master/user/atkuser/bipartite_graph_final")
val HDFS_SINGLE_FILE_LOCATION = readLine()

if (HDFS_SINGLE_FILE_LOCATION.length() > 0) {
  val conf = new Configuration()
  val fs = FileSystem.get(new Configuration())
  def getFullyQualifiedPath(path: String, fs: FileSystem): Path = fs.makeQualified(Path.getPathWithoutSchemeAndAuthority(new Path(path)))
  val src = getFullyQualifiedPath(HDFS_OUTPUT_LOCATION, fs)
  var dest = getFullyQualifiedPath(HDFS_SINGLE_FILE_LOCATION, fs)
  val res = FileUtil.copyMerge(fs, src, fs, dest, true, conf, null)
  if (res) println("File merge was successful")
  else println("Failed to merge source files")
}
sys.exit()
