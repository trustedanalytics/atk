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


package org.trustedanalytics.atk.giraph.algorithms.als;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
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
import org.apache.mahout.math.*;
import org.trustedanalytics.atk.giraph.config.cf.CollaborativeFilteringConfig;
import org.trustedanalytics.atk.giraph.config.cf.CollaborativeFilteringConfiguration;
import org.trustedanalytics.atk.giraph.io.CFVertexId;
import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable;
import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable.EdgeType;
import org.trustedanalytics.atk.giraph.io.MessageData4CFWritable;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable.VertexType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

/**
 * Alternating Least Squares with Bias for collaborative filtering
 * The algorithms presented in
 * (1) Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan. Large-Scale
 * Parallel Collaborative Filtering for the Netflix Prize. 2008.
 * (2) Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
 * Filtering Model. In ACM KDD 2008. (Equation 5)
 */
@Algorithm(
    name = "Alternating Least Squares with Bias"
)
public class AlternatingLeastSquaresComputation extends BasicComputation<CFVertexId, VertexData4CFWritable,
    EdgeData4CFWritable, MessageData4CFWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "als.maxSupersteps";
    /** Custom argument for feature dimension */
    public static final String FEATURE_DIMENSION = "als.featureDimension";
    /**
     * Custom argument for regularization parameter: lambda
     * f = L2_error + lambda*Tikhonov_regularization
     */
    public static final String LAMBDA = "als.lambda";
    /** Custom argument to turn on/off bias */
    public static final String BIAS_ON = "als.biasOn";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "als.convergenceThreshold";
    /** Custom argument for checking bi-directional edge or not (default: false) */
    public static final String BIDIRECTIONAL_CHECK = "als.bidirectionalCheck";
    /** Custom argument for maximum edge weight value */
    public static final String MAX_VAL = "als.maxVal";
    /** Custom argument for minimum edge weight value */
    public static final String MIN_VAL = "als.minVal";
    /**
     * Custom argument for learning curve output interval (default: every iteration)
     * Since each ALS iteration is composed by 2 super steps, one iteration
     * means two super steps.
     */
    public static final String LEARNING_CURVE_OUTPUT_INTERVAL = "als.learningCurveOutputInterval";

    /** Aggregator name for sum of cost on training data */
    private static String SUM_TRAIN_COST = "cost_train";
    /** Aggregator name for sum of L2 error on validate data */
    private static String SUM_VALIDATE_ERROR = "validate_rmse";
    /** Aggregator name for sum of L2 error on test data */
    private static String SUM_TEST_ERROR = "test_rmse";
    /** Aggregator name for number of left vertices */
    private static String SUM_LEFT_VERTICES = "num_left_vertices";
    /** Aggregator name for number of right vertices */
    private static String SUM_RIGHT_VERTICES = "num_right_vertices";
    /** Aggregator name for number of training edges */
    private static String SUM_TRAIN_EDGES = "num_train_edges";
    /** Aggregator name for number of validate edges */
    private static String SUM_VALIDATE_EDGES = "num_validate_edges";
    /** Aggregator name for number of test edges */
    private static String SUM_TEST_EDGES = "num_test_edges";
    /** RMSE value of previous iteration for convergence monitoring */
    private static String PREV_RMSE = "prev_rmse";

    /** Number of super steps */
    private int maxSupersteps = 20;
    /** Feature dimension */
    private int featureDimension = 20;
    /** The regularization parameter */
    private float lambda = 0f;
    /** Bias on/off switch */
    private boolean biasOn = false;
    /** Maximum edge weight value */
    private float maxVal = Float.POSITIVE_INFINITY;
    /** Minimum edge weight value */
    private float minVal = Float.NEGATIVE_INFINITY;
    /** Iteration interval to output learning curve */
    private int learningCurveOutputInterval = 1;

    @Override
    public void preSuperstep() {

        CollaborativeFilteringConfig config = new CollaborativeFilteringConfiguration(getConf()).getConfig();
        maxSupersteps = config.maxIterations();
        featureDimension = config.numFactors();
        lambda = config.lambda();
        biasOn = config.biasOn();
        maxVal = config.maxValue();
        minVal = config.minValue();
        learningCurveOutputInterval = config.learningCurveInterval();
    }

    /**
     * Initialize vertex, collect graph statistics and send out messages
     *
     * @param vertex of the graph
     */
    private void initialize(Vertex<CFVertexId, VertexData4CFWritable, EdgeData4CFWritable> vertex) {
        // initialize vertex data: vertex type, bias, and vector
        vertex.getValue().setBias(0d);
        vertex.getValue().setType(vertex.getId().isUser() ? VertexType.User: VertexType.Item);

        double sum = 0d;
        int numTrain = 0;
        for (Edge<CFVertexId, EdgeData4CFWritable> edge : vertex.getEdges()) {
            EdgeType et = edge.getValue().getType();
            if (et == EdgeType.TRAIN) {
                double weight = edge.getValue().getWeight();
                if (weight < minVal || weight > maxVal) {
                    throw new IllegalArgumentException(String.format("Vertex ID: %s has an edge with weight value " +
                        "out of the range of [%f, %f].", vertex.getId().getValue(), minVal, maxVal));
                }
                sum += weight;
                numTrain++;
            }
        }

        Random rand = new Random(vertex.getId().seed());
        double[] values = new double[featureDimension];
        values[0] = 0d;
        if (numTrain > 0) {
            values[0] = sum / numTrain;
        }
        for (int i = 1; i < featureDimension; i++) {
            double sample = rand.nextDouble() * values[0];
            values[i] = sample;
        }
        vertex.getValue().setVector(new DenseVector(values));

        // collect graph statistics and send out messages
        VertexType vt = vertex.getValue().getType();
        switch (vt) {
        case User:
            aggregate(SUM_LEFT_VERTICES, new LongWritable(1));
            break;
        case Item:
            aggregate(SUM_RIGHT_VERTICES, new LongWritable(1));
            long numTrainEdges = 0L;
            long numValidateEdges = 0L;
            long numTestEdges = 0L;
            for (Edge<CFVertexId, EdgeData4CFWritable> edge : vertex.getEdges()) {
                EdgeType et = edge.getValue().getType();
                switch (et) {
                case TRAIN:
                    numTrainEdges++;
                    break;
                case VALIDATE:
                    numValidateEdges++;
                    break;
                case TEST:
                    numTestEdges++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown recognized edge type: " + et.toString());
                }
                // send out messages
                MessageData4CFWritable newMessage = new MessageData4CFWritable(vertex.getValue(), edge.getValue());
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
            if (numTrainEdges > 0) {
                aggregate(SUM_TRAIN_EDGES, new LongWritable(numTrainEdges));
            }
            if (numValidateEdges > 0) {
                aggregate(SUM_VALIDATE_EDGES, new LongWritable(numValidateEdges));
            }
            if (numTestEdges > 0) {
                aggregate(SUM_TEST_EDGES, new LongWritable(numTestEdges));
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown recognized vertex type: " + vt.toString());
        }
    }

    /**
     * Compute bias
     *
     * @param value of type Vector
     * @param messages of type Iterable
     * @return bias of type double
     */
    private double computeBias(Vector value, Iterable<MessageData4CFWritable> messages) {
        double errorOnTrain = 0d;
        int numTrain = 0;
        for (MessageData4CFWritable message : messages) {
            EdgeType et = message.getType();
            if (et == EdgeType.TRAIN) {
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = otherBias + value.dot(vector);
                double e = weight - predict;
                errorOnTrain += e;
                numTrain++;
            }
        }
        double bias = 0d;
        if (numTrain > 0) {
            bias = errorOnTrain / ((1 + lambda) * numTrain);
        }
        return bias;
    }

    @Override
    public void compute(Vertex<CFVertexId, VertexData4CFWritable, EdgeData4CFWritable> vertex,
        Iterable<MessageData4CFWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initialize(vertex);
            vertex.voteToHalt();
            return;
        }

        Vector currentValue = vertex.getValue().getVector();
        double currentBias = vertex.getValue().getBias();
        // update aggregators every (2 * interval) super steps
        if ((step % (2 * learningCurveOutputInterval)) == 0) {
            double errorOnTrain = 0d;
            double errorOnValidate = 0d;
            double errorOnTest = 0d;
            int numTrain = 0;
            for (MessageData4CFWritable message : messages) {
                EdgeType et = message.getType();
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = currentBias + otherBias + currentValue.dot(vector);
                double e = weight - predict;
                switch (et) {
                case TRAIN:
                    errorOnTrain += e * e;
                    numTrain++;
                    break;
                case VALIDATE:
                    errorOnValidate += e * e;
                    break;
                case TEST:
                    errorOnTest += e * e;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown recognized edge type: " + et.toString());
                }
            }
            double costOnTrain = 0d;
            if (numTrain > 0) {
                costOnTrain = errorOnTrain / numTrain + lambda * (currentBias * currentBias +
                    currentValue.dot(currentValue));
            }
            aggregate(SUM_TRAIN_COST, new DoubleWritable(costOnTrain));
            aggregate(SUM_VALIDATE_ERROR, new DoubleWritable(errorOnValidate));
            aggregate(SUM_TEST_ERROR, new DoubleWritable(errorOnTest));
        }

        // update vertex value
        if (step < maxSupersteps) {
            // xxt records the result of x times x transpose
            Matrix xxt = new DenseMatrix(featureDimension, featureDimension);
            xxt = xxt.assign(0d);
            // xr records the result of x times rating
            Vector xr = currentValue.clone().assign(0d);
            int numTrain = 0;
            for (MessageData4CFWritable message : messages) {
                EdgeType et = message.getType();
                if (et == EdgeType.TRAIN) {
                    double weight = message.getWeight();
                    Vector vector = message.getVector();
                    double otherBias = message.getBias();
                    xxt = xxt.plus(vector.cross(vector));
                    xr = xr.plus(vector.times(weight - currentBias - otherBias));
                    numTrain++;
                }
            }
            xxt = xxt.plus(new DiagonalMatrix(lambda * numTrain, featureDimension));
            Matrix bMatrix = new DenseMatrix(featureDimension, 1).assignColumn(0, xr);
            Vector value = new QRDecomposition(xxt).solve(bMatrix).viewColumn(0);
            vertex.getValue().setVector(value);

            // update vertex bias
            if (biasOn) {
                double bias = computeBias(value, messages);
                vertex.getValue().setBias(bias);
            }

            // send out messages
            for (Edge<CFVertexId, EdgeData4CFWritable> edge : vertex.getEdges()) {
                MessageData4CFWritable newMessage = new MessageData4CFWritable(vertex.getValue(), edge.getValue());
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
        }

        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link AlternatingLeastSquaresComputation}. It registers required aggregators.
     */
    public static class AlternatingLeastSquaresMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_TRAIN_COST, DoubleSumAggregator.class);
            registerAggregator(SUM_VALIDATE_ERROR, DoubleSumAggregator.class);
            registerAggregator(SUM_TEST_ERROR, DoubleSumAggregator.class);
            registerPersistentAggregator(SUM_LEFT_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_RIGHT_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TRAIN_EDGES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_VALIDATE_EDGES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TEST_EDGES, LongSumAggregator.class);
        }

        @Override
        public void compute() {
            // check convergence at every (2 * interval) super steps
            int learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);
            long step = getSuperstep();
            if (step < 3 || (step % (2 * learningCurveOutputInterval)) == 0) {
                return;
            }
            // calculate rmse on validate data
            DoubleWritable sumValidateError = getAggregatedValue(SUM_VALIDATE_ERROR);
            LongWritable numValidateEdges = getAggregatedValue(SUM_VALIDATE_EDGES);
            double validateRmse = 0d;
            if (numValidateEdges.get() > 0) {
                validateRmse = sumValidateError.get() / numValidateEdges.get();
                validateRmse = Math.sqrt(validateRmse);
            }
            sumValidateError.set(validateRmse);
            // calculate rmse on test data
            DoubleWritable sumTestError = getAggregatedValue(SUM_TEST_ERROR);
            LongWritable numTestEdges = getAggregatedValue(SUM_TEST_EDGES);
            double testRmse = 0d;
            if (numTestEdges.get() > 0) {
                testRmse = sumTestError.get() / numTestEdges.get();
                testRmse = Math.sqrt(testRmse);
            }
            sumTestError.set(testRmse);
            // evaluate convergence condition
            if (step >= 3 + (2 * learningCurveOutputInterval)) {
                float prevRmse = getConf().getFloat(PREV_RMSE, 0f);
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                if (Math.abs(prevRmse - validateRmse) < threshold) {
                    haltComputation();
                }
            }
            getConf().setFloat(PREV_RMSE, (float) validateRmse);
        }
    }

    /**
     * This is an aggregator writer for ALS, which after each superstep will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class AlternatingLeastSquaresAggregatorWriter implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String FILENAME;
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        /** Last superstep number*/
        private long lastStep = -1L;

        private ImmutableClassesGiraphConfiguration conf;

        public static String getFilename() {
            return FILENAME;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void initialize(Context context, long applicationAttempt) throws IOException {
            setFilename(applicationAttempt);
            String outputDir = context.getConfiguration().get("mapred.output.dir");
            Path p = new Path(outputDir + "/" + FILENAME);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            output = fs.create(p, true);
        }

        /**
         * Set filename written to
         *
         * @param applicationAttempt of type long
         */
        private static void setFilename(long applicationAttempt) {
            FILENAME = "cf-learning-report";
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            long realStep = lastStep;
            int learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);

            // collect aggregator data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            if (realStep == 0) {
                // output graph statistics
                long leftVertices = Long.parseLong(map.get(SUM_LEFT_VERTICES));
                long rightVertices = Long.parseLong(map.get(SUM_RIGHT_VERTICES));
                long trainEdges = Long.parseLong(map.get(SUM_TRAIN_EDGES)) * 2;
                long validateEdges = Long.parseLong(map.get(SUM_VALIDATE_EDGES)) * 2;
                long testEdges = Long.parseLong(map.get(SUM_TEST_EDGES)) * 2;
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d (left: %d, right: %d)%n",
                    leftVertices + rightVertices, leftVertices, rightVertices));
                output.writeBytes(String.format("Number of edges: %d (train: %d, validate: %d, test: %d)%n",
                    trainEdges + validateEdges + testEdges, trainEdges, validateEdges, testEdges));
                output.writeBytes("\n");
                // output ALS configuration
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
                int featureDimension = getConf().getInt(FEATURE_DIMENSION, 20);
                float lambda = getConf().getFloat(LAMBDA, 0f);
                boolean biasOn = getConf().getBoolean(BIAS_ON, false);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                boolean bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
                float maxVal = getConf().getFloat(MAX_VAL, Float.POSITIVE_INFINITY);
                float minVal = getConf().getFloat(MIN_VAL, Float.NEGATIVE_INFINITY);
                output.writeBytes("======ALS Configuration======\n");
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes(String.format("featureDimension: %d%n", featureDimension));
                output.writeBytes(String.format("lambda: %f%n", lambda));
                output.writeBytes(String.format("biasOn: %b%n", biasOn));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("bidirectionalCheck: %b%n", bidirectionalCheck));
                output.writeBytes(String.format("maxVal: %f%n", maxVal));
                output.writeBytes(String.format("minVal: %f%n", minVal));
                output.writeBytes(String.format("learningCurveOutputInterval: %d%n", learningCurveOutputInterval));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (realStep > 0 && realStep % (2 * learningCurveOutputInterval) == 0) {
                // output learning progress
                double trainCost = Double.parseDouble(map.get(SUM_TRAIN_COST));
                double validateRmse = Double.parseDouble(map.get(SUM_VALIDATE_ERROR));
                double testRmse = Double.parseDouble(map.get(SUM_TEST_ERROR));
                output.writeBytes(String.format("superstep = %d%c", realStep, '\t'));
                output.writeBytes(String.format("cost(train) = %f%c", trainCost, '\t'));
                output.writeBytes(String.format("rmse(validate) = %f%c", validateRmse, '\t'));
                output.writeBytes(String.format("rmse(test) = %f%n", testRmse));
            }
            output.flush();
            lastStep = superstep;
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
