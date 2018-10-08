package cn.mastercom.util;

import org.nd4j.linalg.api.ndarray.INDArray;

/**
 * Created by Kwong on 2018/8/31.
 */
public class NDArrayUtils {

    public static Double[] toArray(INDArray ndArray){
        Double[] result = new Double[ndArray.length()];
        for (int i = 0; i < ndArray.length(); i++){
            result[i] = ndArray.getDouble(i);
        }
        return result;
    }

    public static double[] toArray2(INDArray ndArray){
        double[] result = new double[ndArray.length()];
        for (int i = 0; i < ndArray.length(); i++){
            result[i] = ndArray.getDouble(i);
        }
        return result;
    }
}
