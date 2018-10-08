package cn.mastercom.dl4j;

import cn.mastercom.api.impl.AbstractSavable;
import cn.mastercom.bean.EciMrData;
import cn.mastercom.util.NDArrayUtils;
import lombok.Cleanup;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FileSystem;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static cn.mastercom.constants.TrainingConstant.*;
import static cn.mastercom.constants.TrainingConstant.NUM_HIDDEN_NODE2;
import static cn.mastercom.util.FileUtils.getInputStreamFrom;
import static cn.mastercom.util.FileUtils.getOutputStreamTo;
import static cn.mastercom.util.ValueUtils.*;

/**
 * 管理 网络模型 和 相关配置
 * Created by Kwong on 2018/8/28.
 */
@Log4j
public class EciBuildingModel extends AbstractSavable{

    private int eci;

    private MultiLayerNetwork network;

    @Setter @Getter private EciBuildingModelConfig config;

    public EciBuildingModel(@NonNull int eci, @NonNull String outputPath, @NonNull FileSystem fs){
        this(eci, outputPath, fs, null);
    }

    public EciBuildingModel(@NonNull int eci, @NonNull String outputPath, @NonNull FileSystem fs, EciBuildingModelConfig config){
        super(outputPath.endsWith(File.separator) ? outputPath + eci + "zip" : outputPath + File.separator  + eci + ".zip", fs);
        this.eci = eci;
        this.config = config;
        if (this.config == null){
            this.config = new EciBuildingModelConfig(outputPath.endsWith(File.separator) ? outputPath + eci + ".txt" : outputPath + File.separator  + eci + ".txt", fs);
        }
    }

    @Override
    public boolean isLoad() {
        return super.isLoad() && config.isLoad();
    }

    @Override
    public boolean exists() {
        return super.exists() && config.exists();
    }

    @Override
    public boolean delete() {
        return super.delete() && config.delete();
    }

    public void save(){
        //保存配置
        config.save();
        //保存模型
        try(OutputStream modelOs = getOutputStreamTo(savingPath, fileSystem)){
            ModelSerializer.writeModel(network, modelOs, true);
        } catch (IOException e) {
            log.error("模型保存失败", e);
        }
    }

    public void load(){

        if (config == null) {
            throw new NullPointerException();
        } else if (!config.isLoad()){
            config.load();
        }
        //只有配置加载好了才能加载网络模型
        if (config.isLoad()){
            network = createIfNotExists(config.getFeatureList().size(), config.getLabelList().size());
            setLoad(true);
        }
    }

    private MultiLayerNetwork create(int inNum, int outNum) {
        if (inNum <= 0 || outNum <= 0) return  null;
        MultiLayerNetwork network = new MultiLayerNetwork(new NeuralNetConfiguration.Builder()
                .seed(SEED)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).learningRate(LEARNING_RATE)
                .weightInit(WeightInit.XAVIER)
                .updater(new Adam())
                .list()
                .layer(0, new DenseLayer.Builder().nIn(inNum).nOut(NUM_HIDDEN_NODE1)
                        .activation(Activation.LEAKYRELU).build())
                .layer(1, new DenseLayer.Builder().nIn(NUM_HIDDEN_NODE1).nOut(NUM_HIDDEN_NODE2)
                        .activation(Activation.LEAKYRELU).build())
                .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
                        .activation(Activation.SOFTMAX)
                        .nIn(NUM_HIDDEN_NODE2).nOut(outNum).build())
                .pretrain(false).backprop(true).build()
        );
        network.init();
        return network;
    }

    private MultiLayerNetwork createIfNotExists(int featureNum, int labelNum){
        MultiLayerNetwork network = null;
        if (exists()) {
            try {
                network = load0();
            } catch (IOException e) {
               log.error(String.format("从【%s】加载模型失败", savingPath), e);
            }
            if (network == null) {
                network = create(featureNum, labelNum);
            }
        } else {
            log.info(String.format("从【%s】找不到模型, 创建新的模型", savingPath));
            network = create(featureNum, labelNum);
        }
        return network;
    }

    private MultiLayerNetwork load0() throws IOException {
        @NonNull @Cleanup InputStream is = getInputStreamFrom(savingPath, fileSystem);
        return ModelSerializer.restoreMultiLayerNetwork(is, true);
    }

    /**
     * 训练
     * @param dataSetIterator
     * @param isUpdateConfig
     */
    public void fit(@NonNull DataSetIterator dataSetIterator, boolean isUpdateConfig) {
        if ((dataSetIterator instanceof MrDataSetIterator) && checkNetworkInit()){

            while (dataSetIterator.hasNext()) {

                DataSet trainingds = dataSetIterator.next();
                trainingds.shuffle();

                INDArray inputfeaure = trainingds.getFeatureMatrix();
                INDArray inputlabel = trainingds.getLabels();
                int realBatchSize = trainingds.numExamples();

                for (int i = 0; i < EPOCHS; i++) {
                    for (int j = 0; j < realBatchSize - MINI_BATCH_SIZE; j = j + MINI_BATCH_SIZE) {
                        network.fit(new DataSet(inputfeaure.get(NDArrayIndex.interval(j, j + MINI_BATCH_SIZE), NDArrayIndex.all()),
                                inputlabel.get(NDArrayIndex.interval(j, j + MINI_BATCH_SIZE), NDArrayIndex.all())));
                    }
                }

                if (isUpdateConfig){
                    NormalizerStandardize standardize = (NormalizerStandardize)dataSetIterator.getPreProcessor();
                    updateConfig(NDArrayUtils.toArray2(standardize.getMean()), NDArrayUtils.toArray2(standardize.getStd()));
                }
            }
        }
    }

    /**
     * 预测
     */
    public int predict(EciMrData eciMrData){

        if (eciMrData.getEci() != eci){
            log.warn(String.format("预测数据eci【%s】与当前模型的eci【%s】不一致", eciMrData.getEci(), eci));
            return -1;
        }

        if (!isLoad()){
            load();
        }

        double[] features = getFeatures(config.getFeatureList(), eciMrData);
        double[] labels = toDoubleArray(config.getLabelList());
        double[] featureMean = toDoubleArray(config.getColMean());
        double[] featureStd = toDoubleArray(config.getColStd());

        DataSet dataSet = new DataSet(Nd4j.create(features), Nd4j.create(labels));

        NormalizerStandardize standardize = new NormalizerStandardize(Nd4j.create(featureMean), Nd4j.create(featureStd));
        standardize.fitLabel(false);
        standardize.preProcess(dataSet);

        try {
            INDArray probabilities = network.output(dataSet.getFeatures(), false);
            double max = -1.0;
            int index = 0;
            for (int i = 0; i < probabilities.length(); i++) {
                double temp = probabilities.getDouble(i);
                if (temp > max) {
                    index = i;
                    max = temp;
                }
            }
            int result = (int) labels[index];
            return result;
        } catch (Exception e) {
            log.error(e);
            return -1;
        }
    }

    private boolean checkNetworkInit(){
        if (!isLoad()){
            return false;
        } if (network == null) {
            return false;
        } else if (network.isInitCalled()) {
            return true;
        } else {
            try {
                network.init();
            }catch (Exception e) {
                log.error("网络初始化异常", e);
                return false;
            }
            return true;
        }
    }

    private void updateConfig(@NonNull double[] featureMeans, @NonNull double[] featureStds){
        List<Double> featureMeanList = config.getColMean();
        List<Double> featureStdList = config.getColStd();

        if (featureMeanList == null || featureMeanList.isEmpty()){
            featureMeanList = new ArrayList<>();
            featureStdList = new ArrayList<>();
            for (int i = 0; i < featureMeans.length; i++){
                featureMeanList.add(featureMeans[i]);
                featureStdList.add(featureStds[i]);
            }
            config.setColMean(featureMeanList);
            config.setColStd(featureStdList);
        } else {
            for (int i = 0; i < featureMeanList.size(); i++){
                featureMeanList.set(i, 0.5 * (featureMeanList.get(i) + featureMeans[i]));
                featureStdList.set(i, 0.5 * (featureStdList.get(i) + featureStds[i]));
            }
        }
    }
}
