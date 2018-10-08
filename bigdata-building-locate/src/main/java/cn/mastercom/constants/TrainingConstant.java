package cn.mastercom.constants;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

/**
 * Created by Kwong on 2018/8/17.
 */
public final class TrainingConstant {

    public static final String FEATURE_MARK_SC_RSRP = "LteScRSRP";

    public static final String FEATURE_MARK_SC_RSRQ = "LteScRSRQ";

    public static final String FEATURE_MARK_SC_BSR = "LteScBSR";

    public static final String FEATURE_MARK_SC_TADV = "LteScTadv";

    public static final String FEATURE_MARK_SC_PHR = "LteScPHR";

    public static final String FEATURE_MARK_SC_SINR = "LteScSinrUL";

    public static final String FEATURE_MARK_NC_COUNT = "Nccount0";
    /**
     * 固定的特征
     */
    public static final String[] FIXED_FEATURE_MARKS = new String[]{FEATURE_MARK_SC_RSRP, FEATURE_MARK_SC_RSRQ, FEATURE_MARK_SC_BSR, FEATURE_MARK_SC_TADV, FEATURE_MARK_SC_PHR, FEATURE_MARK_SC_SINR, FEATURE_MARK_NC_COUNT};
    /**
     * 可选的特征
     */
    public static final String OPTION_FEATURE_MARK_SC_AOA = "LteScAOA";

    public static final String OPTION_FEATURE_MARK_NC_RSRP = "LteNcRsrp";

    public static final String OPTION_FEATURE_MARK_NC_RSRQ = "LteNcRsrq";

    //随机数种子，用于结果复现
    public static final int SEED = 12345;
    //epoch数量(全部数据的训练次数)
    public static final int EPOCHS = 400;

    public static final int NUM_HIDDEN_NODE1 = 150;
    public static final int NUM_HIDDEN_NODE2 = 50;
    //private int numOutputs = 3;

    //Batch size: i.e., each epoch has nSamples/batchSize parameter updates
    public static final int MINI_BATCH_SIZE = 200;
    //网络模型学习率
    public static final double LEARNING_RATE = 0.001;

    /**
     * 保存前几的邻区的阈值
     */
    public static final int TOP_NC_THRESHOLD = 3;

    public static final int BATCH_SIZE = 60000;

    public static final int LEAST_BATCH_SIZE = 1000;

    public static String getFeatureMarkNcRsrp(int ncEarf, int ncPci){
        return new StringBuilder().append(ncEarf).append("_").append(ncPci).append("_").append(OPTION_FEATURE_MARK_NC_RSRP).toString();
    }

    public static String getFeatureMarkNcRsrq(int ncEarf, int ncPci){
        return new StringBuilder().append(ncEarf).append("_").append(ncPci).append("_").append(OPTION_FEATURE_MARK_NC_RSRQ).toString();
    }

}
