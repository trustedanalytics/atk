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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with EdgeData4CF
 */
public class EdgeData4CFWritable implements Writable {

    /** The edge type supported by this vertex */
    public enum EdgeType { TRAIN, VALIDATE, TEST };

    /** The weight value at this edge */
    private double weight = 0d;

    /** The type of this vertex */
    private EdgeType type = null;

    /**
     * Default constructor
     */
    public EdgeData4CFWritable() {
    }

    /**
     * Constructor
     *
     * @param type of type EdgeType
     * @param weight of type double
     */
    public EdgeData4CFWritable(EdgeType type, double weight) {
        this.type = type;
        this.weight = weight;
    }

    /**
     * Setter
     *
     * @param type of type EdgeType
     */
    public void setType(EdgeType type) {
        this.type = type;
    }

    /**
     * Getter
     *
     * @return EdgeType
     */
    public EdgeType getType() {
        return type;
    }

    /**
     * Setter
     *
     * @param weight of type double
     */
    public void setWeight(double weight) {
        this.weight = weight;
    }

    /**
     * Getter
     *
     * @return weight of type double
     */
    public double getWeight() {
        return weight;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int idx = in.readInt();
        EdgeType et = EdgeType.values()[idx];
        setType(et);
        setWeight(in.readDouble());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        EdgeType et = getType();
        out.writeInt(et.ordinal());
        out.writeDouble(getWeight());
    }

    /**
     * Read edge data from DataInput
     *
     * @param in of type DataInput
     * @return EdgeDataWritable
     * @throws IOException
     */
    public static EdgeData4CFWritable read(DataInput in) throws IOException {
        EdgeData4CFWritable writable = new EdgeData4CFWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write edge data to DataOutput
     *
     * @param out of type DataOutput
     * @param type of type EdgeType
     * @param weight of type double
     * @throws IOException
     */
    public static void write(DataOutput out, EdgeType type, double weight) throws IOException {
        new EdgeData4CFWritable(type, weight).write(out);
    }

}
