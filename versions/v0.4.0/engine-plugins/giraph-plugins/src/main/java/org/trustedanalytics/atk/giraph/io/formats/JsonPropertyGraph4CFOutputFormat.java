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

import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable.VertexType;
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
 * <code>Long</code> id and <code>VertexData</code> values.
 */
public class JsonPropertyGraph4CFOutputFormat extends TextVertexOutputFormat<LongWritable,
    VertexData4CFWritable, Writable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new JsonPropertyGraph4CFWriter();
    }

    /**
     * VertexWriter that supports vertices with <code>Long</code> id
     * and <code>VertexData</code> values.
     */
    protected class JsonPropertyGraph4CFWriter extends TextVertexWriterToEachLine {

        @Override
        public Text convertVertexToLine(Vertex<LongWritable, VertexData4CFWritable, Writable> vertex)
            throws IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                // add vertex id
                jsonVertex.put(vertex.getId().get());
                // add bias
                JSONArray jsonBiasArray = new JSONArray();
                jsonBiasArray.put(vertex.getValue().getBias());
                jsonVertex.put(jsonBiasArray);
                // add vertex value
                JSONArray jsonValueArray = new JSONArray();
                Vector vector = vertex.getValue().getVector();
                for (int i = 0; i < vector.size(); i++) {
                    jsonValueArray.put(vector.getQuick(i));
                }
                jsonVertex.put(jsonValueArray);
                // add vertex type
                JSONArray jsonTypeArray = new JSONArray();
                VertexType vt = vertex.getValue().getType();
                String vs;
                switch (vt) {
                case User:
                    vs = "L";
                    break;
                case Item:
                    vs = "R";
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unrecognized vertex type: %s", vt.toString()));
                }
                jsonTypeArray.put(vs);
                jsonVertex.put(jsonTypeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString());
        }
    }

}
