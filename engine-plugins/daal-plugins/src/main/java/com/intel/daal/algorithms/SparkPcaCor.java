/*
//  Copyright(C) 2014-2015 Intel Corporation. All Rights Reserved.
//
//  The source code, information  and  material ("Material") contained herein is
//  owned  by Intel Corporation or its suppliers or licensors, and title to such
//  Material remains  with Intel Corporation  or its suppliers or licensors. The
//  Material  contains proprietary information  of  Intel or  its  suppliers and
//  licensors. The  Material is protected by worldwide copyright laws and treaty
//  provisions. No  part  of  the  Material  may  be  used,  copied, reproduced,
//  modified, published, uploaded, posted, transmitted, distributed or disclosed
//  in any way  without Intel's  prior  express written  permission. No  license
//  under  any patent, copyright  or  other intellectual property rights  in the
//  Material  is  granted  to  or  conferred  upon  you,  either  expressly,  by
//  implication, inducement,  estoppel or  otherwise.  Any  license  under  such
//  intellectual  property  rights must  be express  and  approved  by  Intel in
//  writing.
//
//  *Third Party trademarks are the property of their respective owners.
//
//  Unless otherwise  agreed  by Intel  in writing, you may not remove  or alter
//  this  notice or  any other notice embedded  in Materials by Intel or Intel's
//  suppliers or licensors in any way.
*/
package com.intel.daal.algorithms;

import com.intel.daal.algorithms.pca.*;
import com.intel.daal.data_management.data.HomogenNumericTable;
import com.intel.daal.services.DaalContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class SparkPcaCor {
    /* Class containing results of PCA algorithm */

    public static PcaResult runPCA(DaalContext context, JavaPairRDD<Integer, HomogenNumericTable> dataRDD) {
        JavaPairRDD<Integer, PartialResult> partsRDD = computestep1Local(dataRDD);

        return finalizeMergeOnMasterNode(context, partsRDD);
    }

    private static JavaPairRDD<Integer, PartialResult> computestep1Local(JavaPairRDD<Integer, HomogenNumericTable> dataRDD) {
        JavaPairRDD<Integer, PartialResult> partsRDD = dataRDD.mapToPair(
            new PairFunction<Tuple2<Integer, HomogenNumericTable>, Integer, PartialResult>() {
                public Tuple2<Integer, PartialResult> call(Tuple2<Integer, HomogenNumericTable> tup) {
                    DaalContext context = new DaalContext();

                /* Create algorithm to calculate PCA decomposition using Correlation method on local nodes*/
                    DistributedStep1Local pcaLocal = new DistributedStep1Local(context, Double.class, Method.correlationDense);

                /* Set input data on local node */
                    tup._2().unpack(context);
                    pcaLocal.input.set(InputId.data, tup._2());

                /* Compute PCA on local node */
                    PartialResult pres = pcaLocal.compute();
                    pres.pack();

                    context.dispose();

                    return new Tuple2<Integer, PartialResult>(tup._1(), pres);
                }
            });
        return partsRDD;
    }

    private static PcaResult finalizeMergeOnMasterNode(DaalContext context, JavaPairRDD<Integer, PartialResult> partsRDD) {

        /* Create algorithm to calculate PCA decomposition using Correlation method on master node */
        DistributedStep2Master pcaMaster = new DistributedStep2Master(context, Double.class, Method.correlationDense);

        List<Tuple2<Integer, PartialResult>> parts_List = partsRDD.collect();

        /* Add partial results computed on local nodes to the algorithm on master node */
        for (Tuple2<Integer, PartialResult> value : parts_List) {
            value._2().unpack(context);
            pcaMaster.input.add(MasterInputId.partialResults, value._2());
        }

        /* Compute PCA on master node */
        pcaMaster.compute();

        /* Finalize the computations and retrieve PCA results */
        com.intel.daal.algorithms.pca.Result res = pcaMaster.finalizeCompute();

        PcaResult pcaResult = new PcaResult((HomogenNumericTable) res.get(ResultId.eigenVectors),
            (HomogenNumericTable) res.get(ResultId.eigenValues));
        return (pcaResult);
    }
}

