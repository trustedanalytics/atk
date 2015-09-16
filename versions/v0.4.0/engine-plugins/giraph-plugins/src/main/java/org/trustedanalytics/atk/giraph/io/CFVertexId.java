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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CFVertexId implements WritableComparable {

    private String value;

    /** vertex type has been embedded in the ID since it was needed for VID uniqueness */
    private boolean isUserVertex = false;

    public CFVertexId() {
    }

    /**
     *
     * @param value the value of vertex
     * @param isUser true if this is a user vertex; false otherwise
     */
    public CFVertexId(String value, boolean isUser) {
        this.value = value;
        this.isUserVertex = isUser;
    }

    public String getValue() {
        return value;
    }

    public boolean isUser() {
        return isUserVertex;
    }

    public boolean isItem() {
        return false == isUser();
    }

    /**
     *
     * @return the object hash code
     */
    public long seed() {
        return getValue().hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isUserVertex);
        dataOutput.writeUTF(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isUserVertex = dataInput.readBoolean();
        value = dataInput.readUTF();
    }

    @Override
    public int compareTo(Object o) {
        CFVertexId other = (CFVertexId) o;
        if (isSameVertexType(other)) {
            return other.getValue().compareTo(getValue());
        }
        else if(isUser()) {
            return 1;
        }
        else {
            return -1;
        }
    }

    private boolean isSameVertexType(CFVertexId other) {
        return (isUser() && other.isUser()) &&
                (isItem() && other.isItem());
    }

    /**
     * Giraph re-uses writables, sometimes you need to make a copy()
     * @return copy of the current object with same values
     */
    public CFVertexId copy() {
        return new CFVertexId(value, isUserVertex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !getClass().getName().equals(o.getClass().getName())) return false;

        CFVertexId that = (CFVertexId) o;

        if (isUserVertex != that.isUserVertex) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + (isUserVertex ? 1 : 0);
        return result;
    }


    @Override
    public String toString() {
        if (isUserVertex) {
            return "[User:" + value + "]";
        }
        else {
            return "[Item:" + value + "]";
        }
    }
}
