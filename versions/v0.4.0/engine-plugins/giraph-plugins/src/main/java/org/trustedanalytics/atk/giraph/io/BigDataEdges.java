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

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.giraph.edge.*;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.utils.io.BigDataInput;
import org.apache.giraph.utils.io.BigDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * {@link OutEdges} implementation backed by BigData input/output
 * format to support serialization of edges larger than 1GB.
 *
 * {@link ByteArrayEdges} This class is based on Giraph's ByteArrayEdges.
 *
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class BigDataEdges<I extends WritableComparable, E extends Writable>
    extends ConfigurableOutEdges<I, E>
    implements ReuseObjectsOutEdges<I, E> {

  /** Serialized edges. */
  private BigDataOutput serializedEdges;

  /** Number of edges. */
  private int edgeCount;

  @Override
  public void initialize(Iterable<Edge<I, E>> edges) {
    serializedEdges = new BigDataOutput(getConf());
    for (Edge<I, E> edge : edges) {
      try {
        WritableUtils.writeEdge(serializedEdges, edge);
      } catch (IOException e) {
        throw new IllegalStateException("initialize: Failed to serialize " +
            edge);
      }
      ++edgeCount;
    }
  }

  @Override
  public void initialize(int capacity) {
    // We have no way to know the size in bytes used by a certain
    // number of edges.
    initialize();
  }

  @Override
  public void initialize() {
    serializedEdges = new BigDataOutput(getConf());
  }

  @Override
  public void add(Edge<I, E> edge) {
    try {
      WritableUtils.writeEdge(serializedEdges, edge);
    } catch (IOException e) {
      throw new IllegalStateException("add: Failed to write to the new " +
          "byte array");
    }
    ++edgeCount;
  }

  @Override
  public void remove(I targetVertexId) {
    // Note that this is very expensive (deserializes all edges).
    BigDataEdgeIterator iterator = new BigDataEdgeIterator();
    BigDataOutput bigDataOutput = new BigDataOutput(getConf());

    while (iterator.hasNext()) {
      Edge<I, E> edge = iterator.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
       --edgeCount;
      }
      else {
        try {
          WritableUtils.writeEdge(serializedEdges, edge);
        } catch (IOException e) {
          throw new IllegalStateException("initialize: Failed to serialize " +
                  edge);
        }
      }
    }
    serializedEdges = bigDataOutput;
  }

  @Override
  public int size() {
    return edgeCount;
  }


  /**
   * Iterator that reuses the same Edge object.
   */
  private class BigDataEdgeIterator
      extends UnmodifiableIterator<Edge<I, E>> {
    /** Input for processing the bytes */
    private BigDataInput bigDataInput = new BigDataInput(serializedEdges);

    /** Representative edge object. */
    private ReusableEdge<I, E> representativeEdge =
        getConf().createReusableEdge();

    @Override
    public boolean hasNext() {
      return !bigDataInput.endOfInput();
    }

    @Override
    public Edge<I, E> next() {
      try {
        WritableUtils.readEdge(bigDataInput, representativeEdge);
      } catch (IOException e) {
        throw new IllegalStateException("next: Failed on pos " +
            bigDataInput.getPos() + " edge " + representativeEdge);
      }
      return representativeEdge;
    }
  }

  @Override
  public Iterator<Edge<I, E>> iterator() {
    if (edgeCount == 0) {
      return Iterators.emptyIterator();
    } else {
      return new BigDataEdgeIterator();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    BigDataOutput bigDataOutput = new BigDataOutput(getConf());
    bigDataOutput.readFields(in);
    serializedEdges = bigDataOutput;
    edgeCount = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    serializedEdges.write(out);
    out.writeInt(edgeCount);
  }
}
