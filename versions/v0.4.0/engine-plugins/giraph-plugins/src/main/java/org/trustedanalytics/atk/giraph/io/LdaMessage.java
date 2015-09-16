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
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LdaMessage implements Writable {

    private LdaVertexId vid = new LdaVertexId();
    private VectorWritable vectorWritable = new VectorWritable();

    public LdaMessage() {
    }

    public LdaMessage(LdaVertexId vid, Vector vector) {
        this.vid = vid;
        this.vectorWritable.set(vector);
    }

    public LdaVertexId getVid() {
        return vid;
    }

    public Vector getVector() {
        return vectorWritable.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        vid.write(dataOutput);
        vectorWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vid.readFields(dataInput);
        vectorWritable.readFields(dataInput);
    }
}
