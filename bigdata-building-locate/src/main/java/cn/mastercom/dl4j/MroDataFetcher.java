package cn.mastercom.dl4j;

import org.deeplearning4j.datasets.fetchers.BaseDataFetcher;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.indexing.NDArrayIndex;

/**
 * Created by Kwong on 2018/9/12.
 */
public class MroDataFetcher extends BaseDataFetcher {


    final DataSet normalizeDataSet;
    INDArray inputFeature = null;
    INDArray inputLabel = null;

    public MroDataFetcher(DataSet normalizeDataSet) {
        this.normalizeDataSet = normalizeDataSet;
        this.totalExamples = normalizeDataSet.numExamples();
        reset();
    }

    public void init(DataNormalization normalization){
        if (normalization == null){
            normalization = new NormalizerStandardize();
        }

        normalization.fit(normalizeDataSet);
        normalization.preProcess(normalizeDataSet);
    }

    @Override
    public void fetch(int numExamples) {
        if(!this.hasMore()) {
            throw new IllegalStateException("Unable to get more; there are no more data");
        } else {
            if (cursor + numExamples <= totalExamples){
                this.curr = new DataSet(inputFeature.get(NDArrayIndex.interval(cursor, cursor + numExamples), NDArrayIndex.all()),
                        inputLabel.get(NDArrayIndex.interval(cursor, cursor + numExamples), NDArrayIndex.all()));
                cursor += numExamples;
            } else {
                this.curr = new DataSet(inputFeature.get(NDArrayIndex.interval(cursor, totalExamples), NDArrayIndex.all()),
                        inputLabel.get(NDArrayIndex.interval(cursor, totalExamples), NDArrayIndex.all()));
                cursor = totalExamples;
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        normalizeDataSet.shuffle();
        inputFeature = normalizeDataSet.getFeatures();
        inputLabel = normalizeDataSet.getLabels();
    }


}
