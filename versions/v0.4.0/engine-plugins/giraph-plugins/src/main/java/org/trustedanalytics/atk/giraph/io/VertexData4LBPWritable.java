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

package org.trustedanalytics.atk.giraph.io;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Writable to handle serialization of the fields associated with vertex data
 */
public class VertexData4LBPWritable implements Writable {
    /** the vertex type supported by this vertex */
    public enum VertexType { TRAIN, VALIDATE, TEST }

    /** the type of this vertex */
    private VertexType type = null;

    /** prior vector */
    private final VectorWritable priorWritable = new VectorWritable();

    /** posterior vector */
    private final VectorWritable posteriorWritable = new VectorWritable();

    /**
     * Default constructor
     */
    public VertexData4LBPWritable() {
    }

    /**
     * Constructor
     *
     * @param type of type VertexType
     * @param prior of type Vector
     * @param posterior of type Vector
     */
    public VertexData4LBPWritable(VertexType type, Vector prior, Vector posterior) {
        this.type = type;
        priorWritable.set(prior);
        posteriorWritable.set(posterior);
    }

    /**
     * Setter
     *
     * @param type of type VertexType
     */
    public void setType(VertexType type) {
        this.type = type;
    }

    /**
     * Getter
     *
     * @return type of type VertexType
     */
    public VertexType getType() {
        return type;
    }

    /**
     * Getter
     *
     * @return vector of type Vector
     */
    public Vector getPriorVector() {
        return priorWritable.get();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setPriorVector(Vector vector) {
        priorWritable.set(vector);
    }

    /**
     * Getter
     *
     * @return vector of type Vector
     */
    public Vector getPosteriorVector() {
        return posteriorWritable.get();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setPosteriorVector(Vector vector) {
        posteriorWritable.set(vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int idx = in.readInt();
        VertexType vt = VertexType.values()[idx];
        setType(vt);
        priorWritable.readFields(in);
        posteriorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        VertexType vt = getType();
        out.writeInt(vt.ordinal());
        priorWritable.write(out);
        posteriorWritable.write(out);
    }

    /**
     * Read vertex data from DataInput
     *
     * @param in of type DataInput
     * @return VertexDataWritable
     * @throws IOException
     */
    public static VertexData4LBPWritable read(DataInput in) throws IOException {
        VertexData4LBPWritable writable = new VertexData4LBPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type VertexType
     * @param ssPrior of type SequentailAccessSparseVector
     * @param ssPosterior of type SequentailAccessSparseVector
     * @throws IOException
     */
    public static void write(DataOutput out, VertexType type, SequentialAccessSparseVector ssPrior,
        SequentialAccessSparseVector ssPosterior) throws IOException {
        new VertexData4LBPWritable(type, ssPrior, ssPosterior).write(out);
    }

}
