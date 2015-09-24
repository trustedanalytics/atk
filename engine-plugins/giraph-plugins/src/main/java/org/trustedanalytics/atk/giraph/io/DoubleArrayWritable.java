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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * A writable for serializing arrays of doubles
 *
 * Used to serialize dense vectors of doubles efficiently by storing the length
 * of the array and the elements.
 *
 * Some vector writable formats serialize the class name because they can
 * instantiate different classes of vectors (e.g., sparse vs. dense).
 * These formats are more generalizable but less space-efficient.
 */
public class DoubleArrayWritable implements Writable {

    private double[] array;

    public DoubleArrayWritable() {
        this.array = new double[0];
    }

    public DoubleArrayWritable(double[] array) {
        this.array = array;
    }

    public DoubleArrayWritable(Vector vector) {
        this.array = vectorToArray(vector);
    }

    public double[] get() {
        return array;
    }

    public Vector getVector() {
        return new DenseVector(array);
    }

    public void set(double[] array) {
        this.array = array;
    }

    public void set(Vector vector) {
        this.array = vectorToArray(vector);
    }

    public int size() {
        return array.length;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int arraySize = size();
        dataOutput.writeInt(arraySize);
        for (double d : array) {
            dataOutput.writeDouble(d);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int arraySize = dataInput.readInt();
        if (arraySize < 0) throw new IOException("Invalid array size: " + arraySize);

        double[] array = new double[arraySize];
        for (int i = 0; i < arraySize; i++) {
            array[i] = dataInput.readDouble();
        }
        this.array = array;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof DoubleArrayWritable) && Arrays.equals(this.array, ((DoubleArrayWritable) o).get());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.array);
    }

    @Override
    public String toString() {
        return Arrays.toString(this.array);
    }

    private double[] vectorToArray(Vector vector) {
        double[] arr = new double[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            arr[i] = vector.get(i);
        }
        return arr;
    }
}
