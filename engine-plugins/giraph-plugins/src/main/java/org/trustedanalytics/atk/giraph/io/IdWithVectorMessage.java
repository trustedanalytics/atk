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

import org.apache.mahout.math.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated Id
 */
public final class IdWithVectorMessage extends NumberWithVectorWritable<Long> {

    /**
     * Default constructor
     */
    public IdWithVectorMessage() {
        super();
    }

    /**
     * Constructor
     *
     * @param id of type long
     * @param vector of type Vector
     */
    public IdWithVectorMessage(long id, Vector vector) {
        super(id, vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setData(in.readLong());
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getData());
        super.write(out);
    }

}
