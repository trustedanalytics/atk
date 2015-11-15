/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.trustedanalytics.atk.giraph.io.formats;

import org.trustedanalytics.atk.giraph.io.VertexData4LPWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>Long</code> id and <code>VertexData4LP</code> values. Both prior
 * and posterior are output.
 */
public class JsonPropertyGraph4LPOutputFormat extends TextVertexOutputFormat<LongWritable,
    VertexData4LPWritable, Writable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new JsonPropertyGraph4LPWriter();
    }
    /**
     * VertexWriter that supports vertices with <code>Long</code> id
     * and <code>VertexData4LP</code> values.
     */
    protected class JsonPropertyGraph4LPWriter extends TextVertexWriterToEachLine {
        @Override
        public Text convertVertexToLine(Vertex<LongWritable, VertexData4LPWritable, Writable> vertex)
            throws IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                // Add id
                jsonVertex.put(vertex.getId().get());
                // Add prior
                JSONArray jsonPriorArray = new JSONArray();
                Vector prior = vertex.getValue().getPriorVector();
                for (int i = 0; i < prior.size(); i++) {
                    jsonPriorArray.put(prior.getQuick(i));
                }
                jsonVertex.put(jsonPriorArray);
                // Add posterior
                JSONArray jsonPosteriorArray = new JSONArray();
                Vector posterior = vertex.getValue().getPosteriorVector();
                for (int i = 0; i < posterior.size(); i++) {
                    jsonPosteriorArray.put(posterior.getQuick(i));
                }
                jsonVertex.put(jsonPosteriorArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString());
        }
    }
}
