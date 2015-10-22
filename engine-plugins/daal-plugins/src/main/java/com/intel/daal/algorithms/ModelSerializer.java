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

import com.intel.daal.algorithms.linear_regression.Model;
import com.intel.daal.services.DaalContext;

import java.nio.ByteBuffer;

public final class ModelSerializer {

    private ModelSerializer() {
    }

    public static com.intel.daal.algorithms.linear_regression.Model deserializeQrModel(DaalContext context, byte[] serializedCObject) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(serializedCObject.length);
        buffer.put(serializedCObject);

        long cModelRef = cDeserializeQrModel(buffer, buffer.capacity());
        Model qrModel = new Model(context, cModelRef);
        return qrModel;
    }

    public static byte[] serializeQrModel(com.intel.daal.algorithms.linear_regression.Model model) {
        ByteBuffer buffer = cSerializeQrModel(model.getCModel());
        byte[] serializedCObject = new byte[buffer.capacity()];
        buffer.position(0);
        buffer.get(serializedCObject);
        cFreeByteBuffer(buffer);
        return serializedCObject;
    }

    protected static native ByteBuffer cSerializeQrModel(long cModel);

    protected static native long cDeserializeQrModel(ByteBuffer buffer, long size);

    private static native void cFreeByteBuffer(ByteBuffer var1);

    static {
      System.loadLibrary("AtkDaalJavaAPI");
    }

}
