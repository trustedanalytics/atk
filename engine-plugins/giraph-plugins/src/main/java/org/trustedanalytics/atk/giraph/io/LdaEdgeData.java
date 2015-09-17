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
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated wordCount
 */
public final class LdaEdgeData implements Writable {

    private Double wordCount = null;
    private final DoubleArrayWritable vectorWritable = new DoubleArrayWritable();

    public LdaEdgeData() {
    }

    /**
     * Constructor
     *
     * @param wordCount of type double
     */
    public LdaEdgeData(double wordCount) {
        this.wordCount = wordCount;
    }

    /**
     * @deprecated this Constructor is the one Titan used but I don't think we need it
     */
    public LdaEdgeData(double wordCount, Vector vector) {
        setWordCount(wordCount);
        setVector(vector);
    }

    public void setWordCount(Double data) {
        this.wordCount = data;
    }

    public Double getWordCount() {
        return wordCount;
    }

    public Vector getVector() {
        return vectorWritable.getVector();
    }

    public void setVector(Vector vector) {
        vectorWritable.set(vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wordCount = in.readDouble();
        vectorWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(wordCount);
        vectorWritable.write(out);
    }

}
