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

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with vertex data
 * of CGD
 */
public class VertexData4CGDWritable extends VertexData4CFWritable {

    /** The gradient value at this vertex */
    private final VectorWritable gradientWritable = new VectorWritable();

    /** The conjugate value at this vertex */
    private final VectorWritable conjugateWritable = new VectorWritable();

    /**
     * Default constructor
     */
    public VertexData4CGDWritable() {
        super();
    }

    /**
     * Constructor
     *
     * @param type of type VertexType
     * @param vector of type Vector
     * @param gradient of type Vector
     * @param conjugate of type Vector
     */
    public VertexData4CGDWritable(VertexType type, Vector vector, Vector gradient, Vector conjugate) {
        super(type, vector);
        gradientWritable.set(gradient);
        conjugateWritable.set(conjugate);
    }

    /**
     * Constructor
     *
     * @param type of type VertexType
     * @param vector of type Vector
     * @param gradient of type Vector
     * @param conjugate of type Vector
     * @param bias of type double
     */
    public VertexData4CGDWritable(VertexType type, Vector vector, Vector gradient, Vector conjugate, double bias) {
        super(type, vector, bias);
        gradientWritable.set(gradient);
        conjugateWritable.set(conjugate);
    }

    /**
     * Getter
     *
     * @return gradient of type Vector
     */
    public Vector getGradient() {
        return gradientWritable.get();
    }

    /**
     * Setter
     *
     * @param gradient of type Vector
     */
    public void setGradient(Vector gradient) {
        gradientWritable.set(gradient);
    }

    /**
     * Getter
     *
     * @return conjugate of type Vector
     */
    public Vector getConjugate() {
        return conjugateWritable.get();
    }

    /**
     * Setter
     *
     * @param conjugate of type Vector
     */
    public void setConjugate(Vector conjugate) {
        conjugateWritable.set(conjugate);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        gradientWritable.readFields(in);
        conjugateWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        gradientWritable.write(out);
        conjugateWritable.write(out);
    }

    /**
     * Read vertex data from DataInput
     *
     * @param in of type DataInput
     * @return VertexDataWritable
     * @throws IOException
     */
    public static VertexData4CGDWritable read(DataInput in) throws IOException {
        VertexData4CGDWritable writable = new VertexData4CGDWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type VertexType
     * @param ssv of type SequentailAccessSparseVector
     * @param ssg of type SequentailAccessSparseVector
     * @param ssc of type SequentailAccessSparseVector
     * @throws IOException
     */
    public static void write(DataOutput out, VertexType type, SequentialAccessSparseVector ssv,
        SequentialAccessSparseVector ssg, SequentialAccessSparseVector ssc) throws IOException {
        new VertexData4CGDWritable(type, ssv, ssg, ssc).write(out);
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type VertexType
     * @param ssv of type SequentailAccessSparseVector
     * @param ssg of type SequentailAccessSparseVector
     * @param ssc of type SequentailAccessSparseVector
     * @param bias of type double
     * @throws IOException
     */
    public static void write(DataOutput out, VertexType type, SequentialAccessSparseVector ssv,
        SequentialAccessSparseVector ssg, SequentialAccessSparseVector ssc, double bias) throws IOException {
        new VertexData4CGDWritable(type, ssv, ssg, ssc, bias).write(out);
    }

}
