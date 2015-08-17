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

package org.trustedanalytics.atk.giraph.io.formats;

import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable;
import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable.EdgeType;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable.VertexType;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJsonPropertyGraph4CFInputFormat extends JsonPropertyGraph4CFInputFormat {

    private RecordReader<LongWritable, Text> rr;
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        when(rr.nextKeyValue()).thenReturn(true);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexData4CFWritable, EdgeData4CFWritable>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    protected TextVertexReader createVertexReader(final RecordReader<LongWritable, Text> rr) {
        return new JsonPropertyGraph4CFReader() {
        @Override
        protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
            return rr;
        }
        };
    }

    @Test
    public void testReadVertex() throws Exception {
        String input = "[1,[],[\"L\"],[[0,1,[\"TR\"]],[2,2,[\"VA\"]],[3,1,[\"TE\"]]]]";

        when(rr.getCurrentValue()).thenReturn(new Text(input));
        TextVertexReader vr = createVertexReader(rr);
        vr.setConf(conf);
        vr.initialize(null, tac);

        assertTrue("Should have been able to read vertex", vr.nextVertex());
        Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> vertex = vr.getCurrentVertex();
        assertEquals(vertex.getNumEdges(), 3);
        assertEquals(1L, vertex.getId().get());
        assertTrue(vertex.getValue().getVector().size() == 0);
        assertTrue(vertex.getValue().getType() == VertexType.User);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(0L)).getWeight(), 0d);
        assertEquals(2.0, vertex.getEdgeValue(new LongWritable(2L)).getWeight(), 0d);
        assertEquals(1.0, vertex.getEdgeValue(new LongWritable(3L)).getWeight(), 0d);
        assertTrue(vertex.getEdgeValue(new LongWritable(0L)).getType() == EdgeType.TRAIN);
        assertTrue(vertex.getEdgeValue(new LongWritable(2L)).getType() == EdgeType.VALIDATE);
        assertTrue(vertex.getEdgeValue(new LongWritable(3L)).getType() == EdgeType.TEST);
    }

    public static class DummyComputation extends NoOpComputation<LongWritable, VertexData4CFWritable,
        EdgeData4CFWritable, Writable> { }

}
