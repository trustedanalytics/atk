package org.trustedanalytics.atk.plugins.orientdbimport

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable
import com.tinkerpop.blueprints.{ Edge => BlueprintsEdge }
import org.apache.spark.atk.graph.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.trustedanalytics.atk.plugins.orientdb.{ DbConfigurations, GraphDbFactory }
import scala.collection.mutable.ArrayBuffer

/**
  *
  * @param sc
  * @param dbConfigurations
  */
class OrientDbEdgeRdd(sc: SparkContext, dbConfigurations: DbConfigurations) extends RDD[Edge](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Edge] = {
    val graph = GraphDbFactory.graphDbConnector(dbConfigurations)
    val partition = split.asInstanceOf[OrientDbPartition]
    val edgeBuffer = new ArrayBuffer[Edge]()
    val schemaReader = new SchemaReader(graph)
    val edgeSchema = schemaReader.importEdgeSchema(partition.className)
    val edges: OrientDynaElementIterable = graph.command(new OCommandSQL(s"select from ${partition.className}")).execute()
    val edgeIterator = edges.iterator().asInstanceOf[java.util.Iterator[BlueprintsEdge]]
    while (edgeIterator.hasNext) {
      val edgeReader = new EdgeReader(graph, edgeSchema)
      val edge = edgeReader.importEdge(edgeIterator.next())
      edgeBuffer += edge
    }
    edgeBuffer.toIterator
  }

  /**
    *
    * @return
    */
  override protected def getPartitions: Array[Partition] = {
    val partitionBuffer = new ArrayBuffer[OrientDbPartition]()
    val graph = GraphDbFactory.graphDbConnector(dbConfigurations)
    val classBaseNames = graph.getEdgeBaseType.getName
    val classIterator = graph.getEdgeType(classBaseNames).getAllSubclasses.iterator()
    var paritionIdx = 0
    while (classIterator.hasNext) {
      val classLabel = classIterator.next().getName
      val clusterIds = graph.getEdgeType(classLabel).getClusterIds
      clusterIds.foreach(id => {
        partitionBuffer += new OrientDbPartition(id, classLabel, paritionIdx)
        paritionIdx += 1
      })
    }
    partitionBuffer.toArray
  }
}
