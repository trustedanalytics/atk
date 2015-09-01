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
import org.apache.spark.mllib.atk.plugins.VectorUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with vertex data of LDA
 */
public class LdaVertexData implements Writable {

    /** The vector value at this vertex */
    private final VectorWritable ldaResult = new VectorWritable(new DenseVector());

    /** The conditional probability of topic given word */
    private final VectorWritable topicGivenWord = new VectorWritable(new DenseVector());

    public LdaVertexData() {
    }

    public void setLdaResult(Vector vector) {
        ldaResult.set(vector);
    }

    public Vector getLdaResult() {
        return ldaResult.get();
    }


    public void setTopicGivenWord(Vector vector) {
        topicGivenWord.set(vector);
    }

    public Vector getTopicGivenWord() {
        return topicGivenWord.get();
    }

    public double[] getLdaResultAsDoubleArray() {
        return VectorUtils.toDoubleArray(getLdaResult());
    }

    public double[] getTopicGivenWordAsDoubleArray() {
        return VectorUtils.toDoubleArray(getTopicGivenWord());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ldaResult.readFields(in);
        topicGivenWord.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ldaResult.write(out);
        topicGivenWord.write(out);
    }
}
