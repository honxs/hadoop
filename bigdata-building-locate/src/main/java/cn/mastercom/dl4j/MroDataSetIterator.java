package cn.mastercom.dl4j;

import cn.mastercom.bean.EciMrData;
import cn.mastercom.util.ValueUtils;
import lombok.NonNull;
import org.deeplearning4j.datasets.iterator.BaseDatasetIterator;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.factory.Nd4j;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static cn.mastercom.util.ValueUtils.getFeatures;

/**
 * Created by Kwong on 2018/9/12.
 */
public class MroDataSetIterator extends BaseDatasetIterator {


    public MroDataSetIterator(int batch, boolean train, @NonNull Iterable<EciMrData> dataIterable, @NonNull List<String> featureList, @NonNull List<Integer> labelList, int maxBatchSize, int minBatchSize){
        super(batch, -1, new MroDataFetcher(createDataSet(train, dataIterable, featureList, labelList, maxBatchSize, minBatchSize)));

        this.preProcessor = new NormalizerStandardize();
        ((MroDataFetcher)this.fetcher).init((NormalizerStandardize)preProcessor);
    }

    private static DataSet createDataSet(boolean train, Iterable<EciMrData> dataIterable, @NonNull List<String> featureList, @NonNull List<Integer> labelList, int maxBatchSize, int minBatchSize) {

        Iterator<EciMrData> iterator = dataIterable.iterator();

        double[][] dLabels = new double[maxBatchSize][labelList.size()];
        double[][] dFeatures = new double[maxBatchSize][featureList.size()];

        int sampleCount = 0;
        boolean isTrain = false;

        for (int i = 0; i < maxBatchSize; i++) {
            if (iterator.hasNext()) {
                EciMrData sample = iterator.next();
                  /*  if (sample.getEci() != this.eci){
                        continue;
                    }*/
                //提取特征
                dFeatures[i] = getFeatures(featureList, sample);

                if (train){
                    //提取标签
                    dLabels[i] = ValueUtils.getLabels(labelList, sample);
                }

                sampleCount += 1;
            } else{
                break;
            }
        }
        //只要满足最小批次大小就能训练
        if (sampleCount >= minBatchSize){
            dLabels = Arrays.copyOf(dLabels, sampleCount);
            dFeatures = Arrays.copyOf(dFeatures, sampleCount);
            isTrain = true;
        }

        if (isTrain) {
            INDArray labels = Nd4j.create(dLabels);
            INDArray features = Nd4j.create(dFeatures);

            return new DataSet(features, labels);

        }
        return new DataSet();
    }
}
