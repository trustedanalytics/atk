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

import org.trustedanalytics.atk.giraph.io.VertexData4LPWritable;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.DenseVector;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class TestJsonPropertyGraph4LPOutputFormat extends JsonPropertyGraph4LPOutputFormat {
    /** Test configuration */
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexData4LPWritable, Writable> conf;
    /**
     * Dummy class to allow ImmutableClassesGiraphConfiguration to be created.
     */
    public static class DummyComputation extends NoOpComputation<LongWritable,VertexData4LPWritable, Writable,
        Writable> { }

    @Before
    public void setUp() {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexData4LPWritable,
            Writable>(giraphConfiguration);
    }

    @Test
    public void testOuputFormat() throws IOException, InterruptedException {
        Text expected = new Text("[1,[2,3],[4,5]]");

        JsonPropertyGraph4LPOutputFormatTestWorker(expected);
    }
  
    private void JsonPropertyGraph4LPOutputFormatTestWorker(Text expected) throws IOException,
        InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        Vertex vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new LongWritable(1L));
        when(vertex.getValue()).thenReturn(new VertexData4LPWritable(new DenseVector(new double[]{2.0, 3.0}),
            new DenseVector(new double[]{4.0, 5.0}), 0d));

        // Create empty iterator == no edges
        when(vertex.getEdges()).thenReturn(new ArrayList<Text>());

        final RecordWriter<Text, Text> tw = mock(RecordWriter.class);
        JsonPropertyGraph4LPWriter writer = new JsonPropertyGraph4LPWriter() {
            @Override
            protected RecordWriter<Text, Text> createLineRecordWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
                return tw;
            }
        };
        writer.setConf(conf);
        writer.initialize(tac);
        writer.writeVertex(vertex);

        verify(tw).write(expected, null);
        verify(vertex, times(0)).getEdges();
    }

}
