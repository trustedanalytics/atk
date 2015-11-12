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
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated number
 *
 * @param <T> a subclass of Number
 */
public abstract class NumberWithVectorWritable<T extends Number> implements Writable {
    /** Data of type T */
    private T data = null;
    /** Data of type Vector */
    private final VectorWritable vectorWritable = new VectorWritable();

    /**
     * Default constructor
     */
    public NumberWithVectorWritable() {
    }

    /**
     * Constructor
     *
     * @param data of type T
     * @param vector of type Vector
     */
    public NumberWithVectorWritable(T data, Vector vector) {
        this.data = data;
        this.vectorWritable.set(vector);
    }

    /**
     * Setter
     *
     * @param data of type T
     */
    public void setData(T data) {
        this.data = data;
    }

    /**
     * Getter
     *
     * @return data of type double
     */
    public T getData() {
        return data;
    }

    /**
     * Getter
     *
     * @return vector of type Vector
     */
    public Vector getVector() {
        return vectorWritable.get();
    }

    /**
     * Setter
     *
     * @param vector of type Vector
     */
    public void setVector(Vector vector) {
        vectorWritable.set(vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vectorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vectorWritable.write(out);
    }

}
