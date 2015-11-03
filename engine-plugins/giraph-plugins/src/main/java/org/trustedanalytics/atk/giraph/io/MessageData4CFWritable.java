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


package org.trustedanalytics.atk.giraph.io;

import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable.EdgeType;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable.VertexType;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with MessageData4CF
 */
public class MessageData4CFWritable implements Writable {

    /** The vertex data for this message */
    private final VertexData4CFWritable vertexDataWritable = new VertexData4CFWritable();

    /** The edge data for this message */
    private final EdgeData4CFWritable edgeDataWritable = new EdgeData4CFWritable();

    /**
     * Default constructor
     */
    public MessageData4CFWritable() {
    }

    /**
     * Constructor
     *
     * @param vector of type Vector
     * @param bias of type double
     * @param type of type EdgeType
     * @param weight of type double
     */
    public MessageData4CFWritable(Vector vector, double bias, EdgeType type, double weight) {
        // vertex type isn't used in message; here uses User as default
        vertexDataWritable.setType(VertexType.User);
        vertexDataWritable.setVector(vector);
        vertexDataWritable.setBias(bias);
        edgeDataWritable.setType(type);
        edgeDataWritable.setWeight(weight);
    }

    /**
     * Constructor
     *
     * @param vertex of type VertexData4CFWritable
     * @param edge of type EdgeData4CFWritable
     */
    public MessageData4CFWritable(VertexData4CFWritable vertex, EdgeData4CFWritable edge) {
        vertexDataWritable.setType(vertex.getType());
        vertexDataWritable.setVector(vertex.getVector());
        vertexDataWritable.setBias(vertex.getBias());
        edgeDataWritable.setType(edge.getType());
        edgeDataWritable.setWeight(edge.getWeight());
    }

    /**
     * Setter
     *
     * @param weight of type double
     */
    public void setWeight(double weight) {
        edgeDataWritable.setWeight(weight);
    }

    /**
     * Getter
     *
     * @return weight of type double
     */
    public double getWeight() {
        return edgeDataWritable.getWeight();
    }

    /**
     * Setter
     *
     * @param type of type EdgeType
     */
    public void setType(EdgeType type) {
        edgeDataWritable.setType(type);
    }

    /**
     * Getter
     *
     * @return type of type EdgeType
     */
    public EdgeType getType() {
        return edgeDataWritable.getType();
    }

    /**
     * Getter
     *
     * @return Vector
     */
    public Vector getVector() {
        return vertexDataWritable.getVector();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setVector(Vector vector) {
        vertexDataWritable.setVector(vector);
    }

    /**
     * Setter
     *
     * @param bias of type double
     */
    public void setBias(double bias) {
        vertexDataWritable.setBias(bias);
    }

    /**
     * Getter
     *
     * @return bias of type double
     */
    public double getBias() {
        return vertexDataWritable.getBias();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        edgeDataWritable.readFields(in);
        vertexDataWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        edgeDataWritable.write(out);
        vertexDataWritable.write(out);
    }

    /**
     * Read message data from DataInput
     *
     * @param in of type DataInput
     * @return MessageDataWritable
     * @throws IOException
     */
    public static MessageData4CFWritable read(DataInput in) throws IOException {
        MessageData4CFWritable writable = new MessageData4CFWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write message data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type EdgeType
     * @param weight of type Double
     * @param ssv of type SequentailAccessSparseVector
     * @param bias of type double
     * @throws IOException
     */
    public static void write(DataOutput out, SequentialAccessSparseVector ssv, double bias,
        EdgeType type, double weight) throws IOException {
        new MessageData4CFWritable(ssv, bias, type, weight).write(out);
    }

}
