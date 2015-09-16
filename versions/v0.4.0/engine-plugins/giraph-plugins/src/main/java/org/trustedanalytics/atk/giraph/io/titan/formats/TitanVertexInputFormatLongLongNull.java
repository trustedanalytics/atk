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
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * TitanHBaseVertexInputFormatLongLongNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>Long</code> vertex values,
 * and <code>Null</code> edge weights.
 */
public class TitanVertexInputFormatLongLongNull extends
        TitanVertexInputFormat<LongWritable, LongWritable, NullWritable> {

    private static final Logger LOG = Logger.getLogger(TitanVertexInputFormatLongLongNull.class);

    /**
     * Create vertex reader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    @Override
    public VertexReader<LongWritable, LongWritable, NullWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new LongLongNullVertexReader(split, context);
    }

    /**
     * Vertex Reader that constructs Giraph vertices from Titan Vertices
     */
    protected static class LongLongNullVertexReader extends TitanVertexReaderCommon<LongWritable, NullWritable> {

        /**
         * Constructs vertex reader
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public LongLongNullVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {

            super(split, context);
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        @Override
        public Vertex<LongWritable, LongWritable, NullWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {
            // Initialize Giraph vertex
            Vertex<LongWritable, LongWritable, NullWritable> vertex = conf.createVertex();
            vertex.initialize(new LongWritable(faunusVertex.getLongId()), new LongWritable(0));

            // Add vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            vertex.setValue(getLongWritableProperty(titanProperties));

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildBlueprintsEdges(faunusVertex);
            addGiraphEdges(vertex, faunusVertex, titanEdges);

            return (vertex);
        }


        /**
         * Create Long writable from value of first Titan property in iterator
         *
         * @param titanProperties Iterator of Titan properties
         * @return Long writable containing value of first property in list
         */
        private LongWritable getLongWritableProperty(Iterator<TitanProperty> titanProperties) {
            long vertexValue = 0;
            if (titanProperties.hasNext()) {
                Object propertyValue = titanProperties.next().getValue();
                try {
                    vertexValue = Long.parseLong(propertyValue.toString());
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to parse long value for property: " + propertyValue);
                }
            }
            return (new LongWritable(vertexValue));
        }
    }
}
