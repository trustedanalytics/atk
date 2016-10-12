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
package hex.tree.drf;

import hex.tree.CompressedTree;
import water.codegen.CodeGenerator;
import water.codegen.CodeGeneratorPipeline;
import water.exceptions.JCodeSB;
import water.util.JCodeGen;
import water.util.SB;
import water.util.SBPrintStream;

public class AtkDrfModel extends DRFModel {

    DRFModel drfModel;

    public AtkDrfModel(DRFModel drfModel) {
        super(drfModel._key, drfModel._parms, drfModel._output);
        this.drfModel = drfModel;
    }

    @Override protected void toJavaPredictBody(SBPrintStream body,
                                               CodeGeneratorPipeline classCtx,
                                               CodeGeneratorPipeline fileCtx,
                                               final boolean verboseCode) {
        final int nclass = _output.nclasses();
        body.ip("java.util.Arrays.fill(preds,0);").nl();
        final String mname = JCodeGen.toJavaId(_key.toString());

        // One forest-per-GBM-tree, with a real-tree-per-class
        for (int t=0; t < _output._treeKeys.length; t++) {
            // Generate score method for given tree
            toJavaForestName(body.i(),mname,t).p(".score0(data,preds);").nl();

            final int treeIdx = t;

            fileCtx.add(new CodeGenerator() {
                @Override
                public void generate(JCodeSB out) {
                    // Generate a class implementing a tree
                    out.nl();
                    toJavaForestName(out.ip("class "), mname, treeIdx).p(" {").nl().ii(1);
                    out.ip("public static void score0(double[] fdata, double[] preds) {").nl().ii(1);
                    for (int c = 0; c < nclass; c++)
                        if (!(binomialOpt() && c == 1 && nclass == 2)) // Binomial optimization
                            toJavaTreeName(out.ip("preds[").p(nclass==1?0:c+1).p("] += "), mname, treeIdx, c).p(".score0(fdata);").nl();
                    out.di(1).ip("}").nl(); // end of function
                    out.di(1).ip("}").nl(); // end of forest class

                    // Generate the pre-tree classes afterwards
                    for (int c = 0; c < nclass; c++) {
                        if (!(binomialOpt() && c == 1 && nclass == 2)) { // Binomial optimization
                            String javaClassName = toJavaTreeName(new SB(), mname, treeIdx, c).toString();
                            CompressedTree ct = _output.ctree(treeIdx, c);
                            SB sb = new SB();
                            new AtkTreeJCodeGen(drfModel, ct, sb, javaClassName, verboseCode).generate();
                            out.p(sb);
                        }
                    }
                }
            });
        }

        toJavaUnifyPreds(body);
    }
}
