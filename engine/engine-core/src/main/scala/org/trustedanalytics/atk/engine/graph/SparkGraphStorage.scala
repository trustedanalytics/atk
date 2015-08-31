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

package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader.TitanReader
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, GraphElement }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.trustedanalytics.atk.NotFoundException
import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.graph._
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, VertexSchema }
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.frame.SparkFrameStorage
import org.trustedanalytics.atk.engine.plugin.SparkInvocation
import org.trustedanalytics.atk.engine.{ GraphBackendStorage, GraphStorage }
import org.trustedanalytics.atk.repository.MetaStore
import com.thinkaurelius.titan.core.TitanGraph
import org.apache.spark.SparkContext
import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

/**
 * Front end for Spark to create and manage graphs using GraphBuilder3
 * @param metaStore Repository for graph and frame meta data.
 * @param backendStorage Backend store the graph database.
 */
class SparkGraphStorage(metaStore: MetaStore,
                        backendStorage: GraphBackendStorage,
                        frameStorage: SparkFrameStorage)
    extends GraphStorage
    with EventLogging {

  /** Lookup a Graph, throw an Exception if not found */
  override def expectGraph(graphRef: GraphReference)(implicit invocation: Invocation): GraphEntity = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(graphRef.graphId)
        }
    }.getOrElse(throw new NotFoundException("graph", graphRef.graphId.toString))
  }

  /**
   * Lookup a Seamless Graph, throw an Exception if not found
   *
   * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
   * The same data can be treated as frames one moment and as a graph the next without any import/export.
   */
  def expectSeamless(graphId: Long): SeamlessGraphMeta = {
    metaStore.withSession("seamless.graph.storage") {
      implicit session =>
        {
          val graph = metaStore.graphRepo.lookup(graphId).getOrElse(throw new NotFoundException("graph", graphId))
          require(graph.isSeamless, "graph existed but did not have the expected storage format")
          val frames = metaStore.frameRepo.lookupByGraphId(graphId)
          SeamlessGraphMeta(graph, frames.toList)
        }
    }
  }

  def expectSeamless(graphRef: GraphReference): SeamlessGraphMeta = expectSeamless(graphRef.id)

  /**
   * Deletes a graph by synchronously deleting its information from the meta-store and asynchronously
   * deleting it from the backend storage.
   * @param graph Graph metadata object.
   */
  override def drop(graph: GraphEntity)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          info(s"dropping graph id:${graph.id}, name:${graph.name}, entityType:${graph.entityType}")
          if (graph.isTitan) {
            backendStorage.deleteUnderlyingTable(graph.storage, quiet = true, inBackground = true)
          }
          metaStore.graphRepo.delete(graph.id).get
        }
    }
  }

  /**
   * @param graph the graph to be copied
   * @param name name to be given to the copied graph
   * @return
   */
  override def copyGraph(graph: GraphEntity, name: Option[String])(implicit invocation: Invocation): GraphEntity = {
    info(s"copying graph id:${graph.id}, name:${graph.name}, entityType:${graph.entityType}")

    val copiedEntity = if (graph.isTitan) {
      val graphCopy = createGraph(GraphTemplate(name, StorageFormats.HBaseTitan))
      backendStorage.copyUnderlyingTable(graph.storage, graphCopy.storage)
      graphCopy
    }
    else {
      val graphCopy = createGraph(GraphTemplate(name))
      val storageName = graphCopy.storage
      val graphMeta = expectSeamless(graph.toReference)
      val framesToCopy = graphMeta.frameEntities.map(_.toReference)
      val copiedFrames = frameStorage.copyFrames(framesToCopy, invocation.asInstanceOf[SparkInvocation].sparkContext)

      metaStore.withSession("spark.graphstorage.copyGraph") {
        implicit session =>
          {
            copiedFrames.foreach(frame => metaStore.frameRepo.update(frame.copy(graphId = Some(graphCopy.id), modifiedOn = new DateTime)))
            metaStore.graphRepo.update(graphCopy.copy(storage = storageName, storageFormat = StorageFormats.SeamlessGraph)).get
          }
      }
    }
    // refresh from DB
    expectGraph(copiedEntity.toReference)
  }

  /**
   * Registers a new graph.
   * @param graph The graph being registered.
   * @return Graph metadata.
   */
  override def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): GraphEntity = {
    metaStore.withSession("spark.graphstorage.create") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(graph.name)
          check match {
            case Some(g) =>
              if (g.statusId == Status.Active) {
                throw new RuntimeException("Graph with same name exists. Create aborted.")
              }
              else {
                metaStore.graphRepo.delete(g.id)
              }
            case _ => //do nothing. it is fine that there is no existing graph with same name.
          }

          val graphEntity = metaStore.graphRepo.insert(graph).get
          val graphBackendName: String = if (graph.isTitan) {
            backendStorage.deleteUnderlyingTable(GraphBackendName.getGraphBackendName(graphEntity), quiet = true)
            GraphBackendName.getGraphBackendName(graphEntity)
          }
          else ""
          metaStore.graphRepo.update(graphEntity.copy(storage = graphBackendName)).get
        }
    }
  }

  override def renameGraph(graph: GraphEntity, newName: String)(implicit invocation: Invocation): GraphEntity = {
    metaStore.withSession("spark.graphstorage.rename") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(Some(newName))
          if (check.isDefined) {
            throw new RuntimeException("Graph with same name exists. Rename aborted.")
          }
          val newGraph = graph.copy(name = Some(newName))
          val renamedGraph = metaStore.graphRepo.update(newGraph).get
          metaStore.graphRepo.updateLastReadDate(renamedGraph).get
        }
    }
  }

  /**
   * Obtain the graph metadata for a range of graph IDs.
   * @return Sequence of graph metadata objects.
   */
  override def getGraphs()(implicit invocation: Invocation): Seq[GraphEntity] = {
    metaStore.withSession("spark.graphstorage.getGraphs") {
      implicit session =>
        {
          metaStore.graphRepo.scanAll().filter(g => g.statusId != Status.Deleted && g.statusId != Status.Deleted_Final && g.name.isDefined)
        }
    }
  }

  override def getGraphByName(name: Option[String])(implicit invocation: Invocation): Option[GraphEntity] = {
    metaStore.withSession("spark.graphstorage.getGraphByName") {
      implicit session =>
        {
          metaStore.graphRepo.lookupByName(name)
        }
    }
  }

  /**
   * Get the metadata for a graph from its unique ID.
   * @param id ID being looked up.
   */
  @deprecated("please use expectGraph() instead")
  override def lookup(id: Long)(implicit invocation: Invocation): Option[GraphEntity] = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(id)
        }
    }
  }

  def incrementIdCounter(graph: GraphReference, idCounter: Long)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.graphstorage.updateIdCounter") {
      implicit session =>
        {
          metaStore.graphRepo.incrementIdCounter(graph.id, idCounter)
        }
    }
  }

  /**
   * Defining an Vertex creates an empty vertex list data frame.
   * @param graphRef unique id for graph meta data (already exists)
   * @param vertexSchema definition for this vertex type
   * @return the meta data for the graph
   */
  def defineVertexType(graphRef: GraphReference, vertexSchema: VertexSchema)(implicit invocation: Invocation): SeamlessGraphMeta = {
    val graph = expectSeamless(graphRef)
    val label = vertexSchema.label
    if (graph.isVertexOrEdgeLabel(label)) {
      throw new IllegalArgumentException(s"The label $label has already been defined in this graph")
    }
    metaStore.withSession("define.vertex") {
      implicit session =>
        {
          val frame = FrameEntity(0, None, vertexSchema, 1, new DateTime, new DateTime, graphId = Some(graphRef.id))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphRef)
  }

  /**
   * Defining an Edge creates an empty edge list data frame.
   * @param graphRef unique id for graph meta data (already exists)
   * @param edgeSchema definition for this edge type
   * @return the meta data for the graph
   */
  def defineEdgeType(graphRef: GraphReference, edgeSchema: EdgeSchema)(implicit invocation: Invocation): SeamlessGraphMeta = {
    val graph = expectSeamless(graphRef)
    if (graph.isVertexOrEdgeLabel(edgeSchema.label)) {
      throw new IllegalArgumentException(s"The label ${edgeSchema.label} has already been defined in this graph")
    }
    else if (!graph.isVertexLabel(edgeSchema.srcVertexLabel)) {
      throw new IllegalArgumentException(s"source vertex type ${edgeSchema.srcVertexLabel} is not defined in this graph")
    }
    else if (!graph.isVertexLabel(edgeSchema.destVertexLabel)) {
      throw new IllegalArgumentException(s"destination vertex type ${edgeSchema.destVertexLabel} is not defined in this graph")
    }

    metaStore.withSession("define.vertex") {
      implicit session =>
        {
          val frame = FrameEntity(0, None, edgeSchema, 1, new DateTime,
            new DateTime, graphId = Some(graphRef.id))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphRef)
  }

  def loadVertexRDD(sc: SparkContext, graphRef: GraphReference, vertexLabel: String)(implicit invocation: Invocation): VertexFrameRdd = {
    val frame = expectSeamless(graphRef).vertexMeta(vertexLabel)
    val frameRdd = frameStorage.loadFrameData(sc, frame)
    new VertexFrameRdd(frameRdd)
  }

  def loadVertexRDD(sc: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): VertexFrameRdd = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isVertexFrame, "frame was not a vertex frame")
    val frameRdd = frameStorage.loadFrameData(sc, frameEntity)
    new VertexFrameRdd(frameRdd)
  }

  def loadEdgeRDD(sc: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): EdgeFrameRdd = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isEdgeFrame, "frame was not an edge frame")
    val frameRdd = frameStorage.loadFrameData(sc, frameEntity)
    new EdgeFrameRdd(frameRdd)
  }

  def loadGbVertices(sc: SparkContext, graph: GraphEntity)(implicit invocation: Invocation): RDD[GBVertex] = {
    val graphEntity = expectGraph(graph.toReference)
    if (graphEntity.isSeamless) {
      val graphEntity = expectSeamless(graph.toReference)
      graphEntity.vertexFrames.map(frame => loadGbVerticesForFrame(sc, frame.toReference)).reduce(_.union(_))
    }
    else {
      // load from Titan
      val titanReaderRdd: RDD[GraphElement] = getTitanReaderRdd(sc, graph)
      import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
      val gbVertices: RDD[GBVertex] = titanReaderRdd.filterVertices()
      gbVertices
    }
  }

  def loadGbEdges(sc: SparkContext, graph: GraphEntity)(implicit invocation: Invocation): RDD[GBEdge] = {
    val graphEntity = expectGraph(graph.toReference)
    if (graphEntity.isSeamless) {
      val graphMeta = expectSeamless(graph.toReference)
      graphMeta.edgeFrames.map(frame => loadGbEdgesForFrame(sc, frame.toReference)).reduce(_.union(_))
    }
    else {
      // load from Titan
      val titanReaderRDD: RDD[GraphElement] = getTitanReaderRdd(sc, graph)
      import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
      val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()
      gbEdges
    }
  }

  def loadGbElements(sc: SparkContext, graph: GraphEntity)(implicit invocation: Invocation): (RDD[GBVertex], RDD[GBEdge]) = {
    val graphEntity = expectGraph(graph.toReference)

    if (graphEntity.isSeamless) {
      val vertexRDD = loadGbVertices(sc, graph)
      val edgeRDD = loadGbEdges(sc, graph)
      (vertexRDD, edgeRDD)
    }
    else {
      //Prevents us from scanning the NoSQL table twice when loading vertices and edges
      //Scanning NoSQL tables is a very expensive operation.
      loadFromTitan(sc, graph)
    }
  }

  def getTitanReaderRdd(sc: SparkContext, graph: GraphEntity): RDD[GraphElement] = {
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)
    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read the graph from Titan
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()
    titanReaderRDD
  }

  /**
   * Create a new Titan graph with the given name.
   * @param gbVertices RDD of vertices.
   * @param gbEdges RDD of edges
   * @param append if true will attempt to append to an existing graph
   */

  def writeToTitan(graph: GraphEntity,
                   gbVertices: RDD[GBVertex],
                   gbEdges: RDD[GBEdge],
                   append: Boolean = false)(implicit invocation: Invocation): Unit = {

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)
    val gb =
      new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = append))

    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

  /**
   * Loads vertices and edges from Titan graph database
   * @param sc Spark context
   * @param graph Graph metadata object
   * @return RDDs of vertices and edges
   */
  def loadFromTitan(sc: SparkContext, graph: GraphEntity): (RDD[GBVertex], RDD[GBEdge]) = {
    val titanReaderRDD: RDD[GraphElement] = getTitanReaderRdd(sc, graph)
    import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

    //Cache data to prevent Titan reader from scanning HBase/Cassandra table twice to read vertices and edges
    titanReaderRDD.persist(StorageLevel.MEMORY_ONLY)
    val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
    val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()
    titanReaderRDD.unpersist()

    (gbVertices, gbEdges)
  }

  /**
   * Get a connection to a TitanGraph
   */
  def titanGraph(graphReference: GraphReference)(implicit invocation: Invocation): TitanGraph = {
    val graph = expectGraph(graphReference)

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph.storage)
    val titanConnector = new TitanGraphConnector(titanConfig)
    titanConnector.connect()
  }

  def loadGbVerticesForFrame(sc: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): RDD[GBVertex] = {
    loadVertexRDD(sc, frameRef).toGbVertexRDD
  }

  def loadGbEdgesForFrame(sc: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): RDD[GBEdge] = {
    loadEdgeRDD(sc, frameRef).toGbEdgeRdd
  }

  def saveVertexRdd(frameRef: FrameReference, vertexFrameRdd: VertexFrameRdd)(implicit invocation: Invocation) = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isVertexFrame, "frame was not a vertex frame")
    frameStorage.saveFrameData(frameEntity.toReference, vertexFrameRdd)
  }

  def saveEdgeRdd(frameRef: FrameReference, edgeFrameRdd: EdgeFrameRdd)(implicit invocation: Invocation) = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isEdgeFrame, "frame was not an edge frame")
    frameStorage.saveFrameData(frameEntity.toReference, edgeFrameRdd)
  }

  /**
   * Set a graph to be deleted on the next execution of garbage collection
   * @param graph graph to delete
   * @param invocation current invocation
   */
  override def scheduleDeletion(graph: GraphEntity)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.graphstorage.scheduleDeletion") {
      implicit session =>
        {
          info(s"marking as ready to delete: graph id:${graph.id}, name:${graph.name}, entityType:${graph.entityType}")
          metaStore.graphRepo.updateReadyToDelete(graph)
        }
    }
  }

}
