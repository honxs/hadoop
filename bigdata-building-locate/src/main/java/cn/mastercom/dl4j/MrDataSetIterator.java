package cn.mastercom.dl4j;

import cn.mastercom.bean.EciMrData;
import cn.mastercom.util.ValueUtils;
import lombok.NonNull;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.DataSetPreProcessor;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.factory.Nd4j;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static cn.mastercom.util.ValueUtils.getFeatures;

/**
 * Created by Kwong on 2018/8/8.
 */
public class MrDataSetIterator implements DataSetIterator {

    private DataSetPreProcessor preProcessor;
    private transient Iterable<EciMrData> iterable;
    private transient Iterator<EciMrData> iterator;
    private int eci;
    /**
     * 最大批次大小，如果数据总量大于这个值，会按这个来拆分成多个批次
     */
    private final int maxBatchSize;
    /**
     * 最小批次大小，如果数据总量小于这个值，将不会被训练
     */
    private final int minBatchSize;

    private final List<String> featureList;
    private final List<Integer> labelList;

    private final LinkedBlockingQueue<DataSet> queue = new LinkedBlockingQueue<>(4);

    public MrDataSetIterator(int eci, @NonNull Iterable<EciMrData> dataIterable, @NonNull List<String> featureList, @NonNull List<Integer> labelList, int maxBatchSize, int minBatchSize){

        if (maxBatchSize < 1)
            throw new IllegalStateException("batchSize can't be < 1");

        this.eci = eci;
        this.iterable = dataIterable;
        this.maxBatchSize = maxBatchSize;
        this.minBatchSize = minBatchSize;
        this.featureList = featureList;
        this.labelList = labelList;

        this.iterator = iterable.iterator();

        NormalizerStandardize standardize = new NormalizerStandardize();
        standardize.fitLabel(false);
        setPreProcessor(standardize);

        fillQueue(true);
    }

    @Override
    public DataSet next(int num) {
        throw new IllegalStateException("next(int) isn't supported for this DataSetIterator");
    }

    @Override
    public int totalExamples() {
        return 0;
    }

    @Override
    public int inputColumns() {
        return featureList.size();
    }

    @Override
    public int totalOutcomes() {
        return 0;
    }

    @Override
    public boolean resetSupported() {
        return false;
    }

    @Override
    public boolean asyncSupported() {
        return false;
    }

    @Override
    public void reset() {
        queue.clear();
        if (iterable != null)
            iterator = iterable.iterator();
    }

    @Override
    public int batch() {
        return maxBatchSize;
    }

    @Override
    public int cursor() {
        return 0;
    }

    @Override
    public int numExamples() {
        return totalExamples();
    }

    @Override
    public void setPreProcessor(DataSetPreProcessor preProcessor) {
        this.preProcessor = preProcessor;
    }

    @Override
    public DataSetPreProcessor getPreProcessor() {
        return preProcessor;
    }

    @Override
    public List<String> getLabels() {
        return null;
    }

    @Override
    public boolean hasNext() {
        fillQueue(false);
        return !queue.isEmpty();
    }

    private void fillQueue(boolean isMinSizeEffective) {
        if (queue.isEmpty()) {

            double[][] dLabels = new double[maxBatchSize][labelList.size()];
            double[][] dFeatures = new double[maxBatchSize][featureList.size()];

            int sampleCount = 0;
            boolean isTrain = false;

            for (int i = 0; i < maxBatchSize; i++) {
                if (iterator.hasNext()) {
                    EciMrData sample = iterator.next();
                    if (sample.getEci() != this.eci){
                        continue;
                    }
                    //提取特征
                    dFeatures[i] = getFeatures(featureList, sample);

                    //提取标签
                    dLabels[i] = ValueUtils.getLabels(labelList, sample);

                    sampleCount += 1;
                } else{
                    break;
                }
            }
            //只要满足最小批次大小就能训练
            if (isMinSizeEffective && sampleCount >= minBatchSize){
                dLabels = Arrays.copyOf(dLabels, sampleCount);
                dFeatures = Arrays.copyOf(dFeatures, sampleCount);
                isTrain = true;
            } else if (sampleCount == maxBatchSize){
                isTrain = true;
            }

            if (isTrain) {
                INDArray labels = Nd4j.create(dLabels);
                INDArray features = Nd4j.create(dFeatures);

                DataSet dataSet = new DataSet(features, labels);
                try {
                    queue.add(dataSet);
                } catch (Exception e) {
                    // live with it
                }
            }
        }
    }

    @Override
    public DataSet next() {
        if (queue.isEmpty())
            throw new NoSuchElementException();

        DataSet dataSet = queue.poll();

        if (preProcessor != null){
            ((NormalizerStandardize)preProcessor).fit(dataSet);
            preProcessor.preProcess(dataSet);
        }

        return dataSet;
    }

    @Override
    public void remove() {
//        throw new IllegalStateException("remove() isn't supported for this DataSetIterator");

    }
}
