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


package org.trustedanalytics.atk.giraph.algorithms.lp;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.Vector;
import org.trustedanalytics.atk.giraph.config.lp.LabelPropagationConfig;
import org.trustedanalytics.atk.giraph.config.lp.LabelPropagationConfiguration;
import org.trustedanalytics.atk.giraph.io.IdWithVectorMessage;
import org.trustedanalytics.atk.giraph.io.VertexData4LPWritable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Label Propagation on Gaussian Random Fields
 * The algorithm presented in:
 * X. Zhu and Z. Ghahramani. Learning from labeled and unlabeled data with
 * label propagation. Technical Report CMU-CALD-02-107, CMU, 2002.
 */
@Algorithm(
    name = "Label Propagation on Gaussian Random Fields"
)
public class LabelPropagationComputation extends BasicComputation<LongWritable, VertexData4LPWritable,
    DoubleWritable, IdWithVectorMessage> {

    /** Aggregator name for sum of cost at each super step */
    private static String SUM_COST = "sum_cost";
    
    /** Cost of previous super step for convergence monitoring */
    private static String PREV_COST = "prev_cost";

    /** Number of super steps */
    private int maxSupersteps;
    
    /** The trade-off parameter between prior and posterior */
    private float lambda;

    /** Default to an invalid value */
    private Vector initialVectorValues;

    @Override
    public void preSuperstep() {
        LabelPropagationConfig config = new LabelPropagationConfiguration(getConf()).getConfig();

        maxSupersteps = config.maxIterations();
        lambda = config.lambda();
        initialVectorValues = null;
    }

    /**
     * initialize vertex and edges
     *
     * @param vertex a graph vertex
     */
    private void initializeVertexEdges(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        
        // normalize prior and initialize posterior
        VertexData4LPWritable vertexValue = vertex.getValue();
        Vector priorValues = vertexValue.getPriorVector();
        if (null != priorValues) {
            priorValues = priorValues.normalize(1d);
            initialVectorValues = priorValues;
        }
        else if (initialVectorValues != null) {
            priorValues = initialVectorValues;
            vertexValue.setLabeledStatus(false);
        }
        else {
         throw new RuntimeException("Vector labels missing from input data for vertex " +
                                    vertex.getId() +
                                    ". Add edge with vertex as first column.");
        }
        vertexValue.setPriorVector(priorValues);
        vertexValue.setPosteriorVector(priorValues.clone());
        vertexValue.setDegree(initializeEdge(vertex));
        
        // send out messages
        IdWithVectorMessage newMessage = new IdWithVectorMessage(vertex.getId().get(), priorValues);
        sendMessageToAllEdges(vertex, newMessage);
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex,
        Iterable<IdWithVectorMessage> messages) throws IOException {
        long superStep = getSuperstep();
        
        if (superStep == 0) {
            initializeVertexEdges(vertex);
            vertex.voteToHalt();
        }
        else if (superStep <= maxSupersteps) {
            VertexData4LPWritable vertexValue = vertex.getValue();
            Vector prior = vertexValue.getPriorVector();
            Vector posterior = vertexValue.getPosteriorVector();
            double degree = vertexValue.getDegree();

            // collect messages sent to this vertex
            HashMap<Long, Vector> map = new HashMap();
            for (IdWithVectorMessage message : messages) {
                map.put(message.getData(), message.getVector());
            }

            // Update belief and calculate cost
            double hi = prior.getQuick(0);
            double fi = posterior.getQuick(0);
            double crossSum = 0d;
            Vector newBelief = posterior.clone().assign(0d);
            
            for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                double weight = edge.getValue().get();
                if (weight <= 0d) {
                    throw new IllegalArgumentException("Vertex ID: " +
                                                       vertex.getId() +
                                                       "has an edge with negative or zero value");
                }
                long targetVertex = edge.getTargetVertexId().get();
                if (map.containsKey(targetVertex)) {
                    Vector tempVector = map.get(targetVertex);
                    newBelief = newBelief.plus(tempVector.times(weight));
                    double fj = tempVector.getQuick(0);
                    crossSum += weight * fi * fj;
                }
            }
            
            double cost = degree * ((1 - lambda) * (Math.pow(fi,2) - crossSum) + 
                                    0.5 * lambda * Math.pow ((fi - hi),2)
                                   );
            aggregate(SUM_COST, new DoubleWritable(cost));

            // Update posterior if the vertex was not processed
            if (vertexValue.wasLabeled() == false) {
                newBelief = (newBelief.times(1 - lambda).plus(prior.times(lambda))).normalize(1d);
                vertexValue.setPosteriorVector(newBelief);
            }

            // Send out messages if not the last step
            if (superStep != maxSupersteps) {
                IdWithVectorMessage newMessage = new IdWithVectorMessage(vertex.getId().get(),
                    vertexValue.getPosteriorVector());
                sendMessageToAllEdges(vertex, newMessage);
            }
        }

        vertex.voteToHalt();
    }

    private double calculateVertexDegree (Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        double degree = 0d;
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().get();
            if (weight <= 0d) {
                throw new IllegalArgumentException(
                                "Vertex ID: " + 
                                vertex.getId() +
                                " has an edge with negative or zero weight value.");
            }
            degree += weight;
        }
        return degree;
    }

    private double initializeEdge(Vertex<LongWritable, VertexData4LPWritable, DoubleWritable> vertex) {
        double degree = calculateVertexDegree(vertex);
        
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getMutableEdges()) {
            edge.getValue().set(edge.getValue().get() / degree);
        }

        return degree;
    }

    /**
     * Master compute associated with {@link LabelPropagationComputation}. It registers required aggregators.
     */
    public static class LabelPropagationMasterCompute extends DefaultMasterCompute {

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_COST, DoubleSumAggregator.class);
        }

        @Override
        public void compute() {
            if (getSuperstep() > 1) {

                DoubleWritable sumCost = getAggregatedValue(SUM_COST);
                double cost = sumCost.get() / getTotalNumVertices();
                sumCost.set(cost);
                
                getConf().setFloat(PREV_COST, (float) cost);
            }
        }
    }

    /**
     * This is an aggregator writer for label propagation, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class LabelPropagationAggregatorWriter implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String filename;
        
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        
        /** Last superstep number */
        private long currentStep = -1L;
        
        /** giraph configuration */
        private ImmutableClassesGiraphConfiguration conf;


        public static String getFilename() {
            return filename;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void initialize(Context context, long applicationAttempt) throws IOException {
            
            setFilename(applicationAttempt);
            String outputDir = context.getConfiguration().get("mapred.output.dir");
            Path outputPath = new Path(outputDir + File.separator + filename);
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            output = fileSystem.create(outputPath, true);
        }

        /**
         * Set filename written to
         *
         * @param applicationAttempt of type long
         */
        private static void setFilename(long applicationAttempt) {
            filename = "lp-learning-report_" + applicationAttempt;
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            LabelPropagationConfig config = new LabelPropagationConfiguration(getConf()).getConfig();

            if (currentStep == 0) {

                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d%n",
                    GiraphStats.getInstance().getVertices().getValue()));
                output.writeBytes(String.format("Number of edges: %d%n",
                    GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");

                output.writeBytes("======LP Configuration======\n");
                output.writeBytes(String.format("lambda: %f%n", config.lambda()));
                output.writeBytes(String.format("convergence threshold: %f%n", config.convergenceThreshold()));
                output.writeBytes(String.format("max iterations: %d%n", config.maxIterations()));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (currentStep > 0) {
                
                // collect aggregator data
                HashMap<String, String> map = new HashMap();
                for (Entry<String, Writable> entry : aggregatorMap) {
                    map.put(entry.getKey(), entry.getValue().toString());
                }
                
                // output learning progress
                output.writeBytes(String.format("superstep = %d%c", currentStep, '\t'));
                double cost = Double.parseDouble(map.get(SUM_COST));
                output.writeBytes(String.format("cost = %f%n", cost));
            }
            output.flush();
            currentStep =  superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

        /**
         * Set the configuration to be used by this object.
         *
         * @param configuration Set configuration
         */
        @Override
        public void setConf(ImmutableClassesGiraphConfiguration configuration) {
            this.conf = configuration;
        }

        /**
         * Return the configuration used by this object.
         *
         * @return Set configuration
         */
        @Override
        public ImmutableClassesGiraphConfiguration getConf() {
            return conf;
        }
    }
}
