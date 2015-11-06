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

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of VertexData4LP (label propagation)
 */
public final class VertexData4LPWritable implements Writable {

    /** prior vector of this vertex */
    private final VectorWritable priorWritable = new VectorWritable();

    /** posterior vector of this vertex */
    private final VectorWritable posteriorWritable = new VectorWritable();

    /** degree of this vertex */
    private double degree = 0;

    /** true if the vertex has been labeled; false otherwise */
    private boolean wasLabeled = false;

    /**
     * Default constructor
     */
    public VertexData4LPWritable() {
    }

    /**
     * Paramerized Constructor
     *
     * @param prior of type vector
     * @param posterior of type vector
     * @param degree of type double
     */
    public VertexData4LPWritable(Vector prior, Vector posterior, double degree) {
        this.priorWritable.set(prior);
        this.posteriorWritable.set(posterior);
        this.degree = degree;

        this.wasLabeled = prior.minValue() >= 0d;
        setStatusAndUnlabeledValues(this.wasLabeled);
    }

    /**
     * Getter
     *
     * @return prior vector
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
     * @return posterior vector
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

    /**
     * Getter
     *
     * @return degree of type double
     */
    public double getDegree() {
        return degree;
    }

    /**
     * Setter
     *
     * @param degree of type double
     */
    public void setDegree(double degree) {
        this.degree = degree;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        priorWritable.readFields(in);
        posteriorWritable.readFields(in);
        degree = in.readDouble();
        wasLabeled = in.readBoolean();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        priorWritable.write(out);
        posteriorWritable.write(out);
        out.writeDouble(degree);
        out.writeBoolean(wasLabeled);
    }

    /**
     * Read vertex data to DataInput
     *
     * @param in of type DataInput
     * @return VertexData4LPWritable
     * @throws IOException
     */
    public static VertexData4LPWritable read(DataInput in) throws IOException {
        VertexData4LPWritable writable = new VertexData4LPWritable();
        writable.readFields(in);

        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param ssv1 of type SequentialAccessSparseVector
     * @param ssv2 of type SequentialAccessSparseVector
     * @param degree of type double
     * @throws IOException
     */
    public static void write(DataOutput out, SequentialAccessSparseVector ssv1,
        SequentialAccessSparseVector ssv2, double degree) throws IOException {
        new VertexData4LPWritable(ssv1, ssv2, degree).write(out);
    }

    /**
     * Returns the status of the vertex
     * @return true if the label has been labeled; false otherwise
     */
    public boolean wasLabeled () {
        return wasLabeled;
    }

    /**
       Sets the status of a vertex
     */
    public void setLabeledStatus (boolean wasLabeled) {
        this.wasLabeled = wasLabeled;
    }
    /**
     * Initialize the labels on vertex
     */
    private void setStatusAndUnlabeledValues(boolean wasLabeled) {

        if (wasLabeled == false) {
            Vector temp = priorWritable.get();
            int size = temp.size();
            for (int i = 0; i < size; i++) {
                temp.set (i, 1/size);
            }
            priorWritable.set(temp);
            posteriorWritable.set(temp);
        }
    }
}
