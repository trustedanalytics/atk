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

package org.trustedanalytics.atk.giraph.io.titan.formats;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

public abstract class TitanVertexReaderCommon<T extends Writable,
                                              K extends  Writable> extends TitanVertexReader<LongWritable, T, K> {
    protected Vertex<LongWritable, T, K> giraphVertex = null;

    /**
     * TitanVertexReaderCore constructor
     *
     * @param split   InputSplit from TableInputFormat
     * @param context task context
     * @throws java.io.IOException
     */
    public TitanVertexReaderCommon(InputSplit split, TaskAttemptContext context) throws IOException {
        super(split, context);
    }

    /**
     * Gets the next Giraph vertex from the input split.
     *
     * @return boolean Returns True, if more vertices available
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        while (getRecordReader().nextKeyValue()) {
            Vertex<LongWritable, T, K> tempGiraphVertex =
                    readGiraphVertex(getConf(), getRecordReader().getCurrentValue());
            if (tempGiraphVertex != null) {
                this.giraphVertex = tempGiraphVertex;
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the current vertex value
     */
    @Override
    public Vertex<LongWritable, T, K> getCurrentVertex() throws IOException,
            InterruptedException {
        return giraphVertex;
    }

    /**
     * Get edges of Giraph vertex
     *
     * @return Iterable of Giraph edges
     * @throws IOException
     */
    protected Iterable<Edge<LongWritable, K>> getEdges() throws IOException {
        return giraphVertex.getEdges();
    }

    /**
     * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
     *
     * @param conf         Giraph configuration with property names, and edge labels to filter
     * @param faunusVertex Faunus vertex
     * @return Giraph vertex
     */
    public abstract Vertex<LongWritable, T, K> readGiraphVertex(
            final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex);

    /**
     * Add edges to Giraph vertex.
     *
     * @param vertex       Giraph vertex to add edges to
     * @param faunusVertex (Faunus (Titan/Hadoop) vertex
     * @param titanEdges   Iterator of Titan edges
     */
    public void addGiraphEdges(Vertex<LongWritable, T, K> vertex,
                                FaunusVertex faunusVertex,
                                Iterator<TitanEdge> titanEdges) {
        while (titanEdges.hasNext()) {
            TitanEdge titanEdge = titanEdges.next();
            Edge<LongWritable, K> edge = getGiraphEdge(faunusVertex, titanEdge);
            vertex.addEdge(edge);
        }
    }

    /**
     * Create Giraph edge from Titan edge
     *
     * @param faunusVertex Faunus (Titan/Hadoop) vertex
     * @param titanEdge    Titan edge
     * @return Giraph edge
     */
    private Edge<LongWritable, K> getGiraphEdge(FaunusVertex faunusVertex, TitanEdge titanEdge) {
        return EdgeFactory.create(new LongWritable(
                titanEdge.getOtherVertex(faunusVertex).getLongId()), (K)NullWritable.get());
    }
}
