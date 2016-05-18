package org.apache.spark.atk.graph

import org.apache.spark.rdd.RDD

object GraphRddImplicits {

  implicit def vertexRDDToVertexRDDFunctions(rdd: RDD[Vertex]) = new VertexRddFunctions(rdd)

  implicit def edgeRDDToEdgeRDDFunctions(rdd: RDD[Edge]) = new EdgeRddFunctions(rdd)
}
