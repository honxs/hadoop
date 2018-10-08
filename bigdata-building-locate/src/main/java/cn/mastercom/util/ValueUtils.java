package cn.mastercom.util;

import cn.mastercom.bean.EciMrData;
import static cn.mastercom.constants.TrainingConstant.*;

import java.util.List;
import java.util.Objects;

/**
 * Created by Kwong on 2018/7/17.
 */
public class ValueUtils {

    public static final int INVALID_INT = -1000000;
    public static final String INVALID_INT_VALUE = "-1000000";

    public static int getValidInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return INVALID_INT;
        }
    }

    public static int getValue(String key, EciMrData eciMrData) {

        Objects.requireNonNull(eciMrData);
        Objects.requireNonNull(key);

        if (key.contains(FEATURE_MARK_SC_RSRP)) return eciMrData.getLteScRSRP();
        if (key.contains(FEATURE_MARK_SC_RSRQ)) return eciMrData.getLteScRSRQ();
        if (key.contains(FEATURE_MARK_SC_BSR)) return eciMrData.getLteScBSR()!=-1000000?eciMrData.getLteScBSR():-10;
        if (key.contains(FEATURE_MARK_SC_TADV)) return eciMrData.getLteScTadv()!=-1000000?eciMrData.getLteScTadv():-10;
        if (key.contains(FEATURE_MARK_SC_PHR)) return eciMrData.getLteScPHR()!=-1000000?eciMrData.getLteScPHR():-10;
        if (key.contains(FEATURE_MARK_SC_SINR)) return eciMrData.getLteScSinrUL()!=-1000000?eciMrData.getLteScSinrUL():-10;
        if (key.contains(FEATURE_MARK_NC_COUNT)) return eciMrData.getNccount0();
        if (key.contains(OPTION_FEATURE_MARK_SC_AOA)) return eciMrData.getLteScAOA()!=-1000000?eciMrData.getLteScAOA():0;

        if (key.contains(getFeatureMarkNcRsrp(eciMrData.getLteNcEarfcn1(), eciMrData.getLteNcPci1()))) return eciMrData.getLteNcRSRP1();
        if (key.contains(getFeatureMarkNcRsrq(eciMrData.getLteNcEarfcn1(), eciMrData.getLteNcPci1()))) return eciMrData.getLteNcRSRQ1();

        if (key.contains(getFeatureMarkNcRsrp(eciMrData.getLteNcEarfcn2(), eciMrData.getLteNcPci2()))) return eciMrData.getLteNcRSRP2();
        if (key.contains(getFeatureMarkNcRsrq(eciMrData.getLteNcEarfcn2(), eciMrData.getLteNcPci2()))) return eciMrData.getLteNcRSRQ2();

        if (key.contains(getFeatureMarkNcRsrp(eciMrData.getLteNcEarfcn3(), eciMrData.getLteNcPci3()))) return eciMrData.getLteNcRSRP3();
        if (key.contains(getFeatureMarkNcRsrq(eciMrData.getLteNcEarfcn3(), eciMrData.getLteNcPci3()))) return eciMrData.getLteNcRSRQ3();

        if (key.contains(getFeatureMarkNcRsrp(eciMrData.getLteNcEarfcn4(), eciMrData.getLteNcPci4()))) return eciMrData.getLteNcRSRP4();
        if (key.contains(getFeatureMarkNcRsrq(eciMrData.getLteNcEarfcn4(), eciMrData.getLteNcPci4()))) return eciMrData.getLteNcRSRQ4();

        if (key.contains(getFeatureMarkNcRsrp(eciMrData.getLteNcEarfcn5(), eciMrData.getLteNcPci5()))) return eciMrData.getLteNcRSRP5();
        if (key.contains(getFeatureMarkNcRsrq(eciMrData.getLteNcEarfcn5(), eciMrData.getLteNcPci5()))) return eciMrData.getLteNcRSRQ5();

        if (key.contains(getFeatureMarkNcRsrp(eciMrData.getLteNcEarfcn6(), eciMrData.getLteNcPci6()))) return eciMrData.getLteNcRSRP6();
        if (key.contains(getFeatureMarkNcRsrq(eciMrData.getLteNcEarfcn6(), eciMrData.getLteNcPci6()))) return eciMrData.getLteNcRSRQ6();

        if (key.contains(OPTION_FEATURE_MARK_NC_RSRP)) return -131;
        else if (key.contains(OPTION_FEATURE_MARK_NC_RSRQ))  return -21;
        else return 0;
    }

    public static double[] toDoubleArray(List<? extends Number> numberList){
        double[] dArray = new double[numberList.size()];

        int i = 0;
        for (Number num : numberList){
            dArray[i++] = Double.parseDouble(num.toString());
        }
        return dArray;
    }

    public static double[] getFeatures(List<String> featureList, EciMrData eciMrData){
        double[] features = new double[featureList.size()];
        for (int ii = 0; ii < featureList.size(); ii++) {
            features[ii] = getValue(featureList.get(ii), eciMrData);
        }
        return features;
    }

    public static double[] getLabels(List<Integer> labelList, EciMrData eciMrData){
        double[] labels = new double[labelList.size()];
        if (eciMrData.getBuilding() <= 0) return labels;
        for (int ii = 0; ii < labelList.size(); ii++) {
            if (eciMrData.getBuilding() == labelList.get(ii)) {
                labels[ii] = 1;
                break;
            }
        }
        return labels;
    }
}
