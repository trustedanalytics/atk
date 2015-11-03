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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LdaVertexId implements WritableComparable {

    private Long value;

    /** vertex type has been embedded in the ID since it was needed for VID uniqueness */
    private boolean isDocument = false;

    public LdaVertexId() {
    }

    /**
     * LDA Vertex ID
     * @param value document name or word value
     * @param isDocument true if Document, false if vertex is for a Word
     */
    public LdaVertexId(Long value, boolean isDocument) {
        this.value = value;
        this.isDocument = isDocument;
    }

    public Long getValue() {
        return value;
    }

    public boolean isDocument() {
        return isDocument;
    }

    public boolean isWord() {
        return !isDocument();
    }

    /**
     * Not sure why but algorithm was using VertexId for getting a seed
     */
    public long seed() {
        return getValue().hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isDocument);
        dataOutput.writeLong(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isDocument = dataInput.readBoolean();
        value = dataInput.readLong();
    }

    @Override
    public int compareTo(Object o) {
        LdaVertexId other = (LdaVertexId) o;
        if (isSameVertexType(other)) {
            return other.getValue().compareTo(getValue());
        }
        else if(isDocument()) {
            return 1;
        }
        else {
            return -1;
        }
    }

    private boolean isSameVertexType(LdaVertexId other) {
        return (isDocument() && other.isDocument()) &&
                (isWord() && other.isWord());
    }

    /**
     * Giraph re-uses writables, sometimes you need to make a copy()
     * @return copy of the current object with same values
     */
    public LdaVertexId copy() {
        return new LdaVertexId(value, isDocument);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !getClass().getName().equals(o.getClass().getName())) return false;

        LdaVertexId that = (LdaVertexId) o;

        if (isDocument != that.isDocument) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + (isDocument ? 1 : 0);
        return result;
    }


    @Override
    public String toString() {
        if (isDocument) {
            return "[D:" + value + "]";
        }
        else {
            return "[W:" + value + "]";
        }
    }
}
