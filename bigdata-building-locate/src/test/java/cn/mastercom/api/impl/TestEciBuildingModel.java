package cn.mastercom.api.impl;

import cn.mastercom.constants.TrainingConstant;
import cn.mastercom.dl4j.EciBuildingModel;
import cn.mastercom.dl4j.EciBuildingModelConfig;
import cn.mastercom.dl4j.MrDataSetIterator;
import cn.mastercom.bean.EciMrData;
import cn.mastercom.util.FileReader;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.*;

import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Created by Kwong on 2018/8/31.
 */
@Log4j
@RunWith(PowerMockRunner.class)
@PrepareForTest({TestEciBuildingModel.class, EciBuildingModel.class})//@Prepare会默认将当前类中所有的class包括内部类嵌套类也prepare了
@PowerMockIgnore(value = {"java.*", "javax.*", "org.*", "akka.*", "scala.*", "com.*", "cn.mastercom.*"})//第三方包全都不要mock，不然会出现意想不到的事情
public class TestEciBuildingModel {

    String modelPath;
    int eci;
    FileSystem fs;
    EciBuildingModel model;

    @Before
    public void setUp() throws IOException {
        modelPath = "E:\\IDEA-workspace\\bigdata-building-locate\\model\\";
        eci = 204883864;
        fs = FileSystem.get(new Configuration());

        model = new EciBuildingModel(eci, modelPath, fs);

    }

    @org.junit.Test
    public void testLoad() {
        model.load();
        Assert.assertTrue(model.isLoad());
        Assert.assertTrue(model.getConfig().getFeatureList().size() > 0);
        for(String featureMark : model.getConfig().getFeatureList()){
            log.info(featureMark);
        }
    }

    @org.junit.Test
    public void testDelete() {
        Assert.assertTrue(model.isLoad() == false);
        Assert.assertTrue(model.delete());
    }

    @org.junit.Test
    public void testSave() {
        Assert.assertTrue(model.exists() == true);
        Assert.assertTrue(model.isLoad() == false);
        model.load();
        Assert.assertTrue(model.isLoad() == true);
        model.delete();
        Assert.assertTrue(model.exists() == false);
        model.save();
        Assert.assertTrue(model.exists() == true);
    }

    @org.junit.Test
    public void testConfigInjection() {
        //组织一个外部配置, 将模型路径下内容删掉
        String configPath = modelPath + eci + ".txt";
        EciBuildingModelConfig config = new EciBuildingModelConfig(configPath, fs);
        Assert.assertTrue(config.exists() == true);
        Assert.assertTrue(config.isLoad() == false);
        config.load();
        Assert.assertTrue(config.isLoad() == true);

        //模型 添加配置
        Assert.assertTrue(model.exists() == false);
        model.setConfig(config);
        Assert.assertTrue(model.isLoad() == false);
        model.load();
        Assert.assertTrue(model.isLoad() == true);
        Assert.assertTrue(model.getConfig().getFeatureList().size() > 0);
        for(String featureMark : model.getConfig().getFeatureList()){
            log.info(featureMark);
        }
    }

    /**
     * TODO 运行此方法需要加上类的注解，其它测试方法不需要加上
     * @throws Exception
     */
    @org.junit.Test
    public void testFit() throws Exception {

        ////////////////////////////////////////////////////////////
     /*   mockStatic(TrainingConstant.class);
        when(TrainingConstant.getFeatureMarkNcRsrp(Mockito.anyInt(), Mockito.anyInt())).thenCallRealMethod();
        when(TrainingConstant.getFeatureMarkNcRsrq(Mockito.anyInt(), Mockito.anyInt())).thenCallRealMethod();
        when(TrainingConstant.createNetwork(Mockito.anyInt(), Mockito.anyInt())).thenAnswer(new Nd4jUIAttach());//绑定到UI*/

        //TrainingConstant.createNetwork(10, 10); //事实证明，调用的类也一定在@Prepare里面，否则打桩无效
        model = PowerMockito.spy(model);
        when(model, "createIfNotExists", Mockito.anyInt(), Mockito.anyInt()).thenAnswer(new Nd4jUIAttach());
        ////////////////////////////////////////////////////////////
        
        //组织一个外部配置
        String configPath = modelPath + eci + ".txt";
        EciBuildingModelConfig config = new EciBuildingModelConfig(configPath, fs);
        config.load();

        //读取数据
        List<EciMrData> data = loadTrainData("C:\\Users\\DELL\\Desktop\\data\\" + eci + "_0703.txt");
        Assert.assertTrue(data.size() > 0);
        log.info("训练集大小为：" + data.size());

        //组织训练集的迭代器
        long start = System.currentTimeMillis();
        MrDataSetIterator mrDataSetIterator = new MrDataSetIterator(eci, data, config.getFeatureList(), config.getLabelList(), TrainingConstant.BATCH_SIZE, TrainingConstant.LEAST_BATCH_SIZE);
        log.info("创建迭代器完成，耗时：" + (System.currentTimeMillis() - start)/1000 + "(s)");

        //模型 添加配置
        model.setConfig(config);
        model.load();
        Assert.assertFalse(model.exists());

        start = System.currentTimeMillis();
        model.fit(mrDataSetIterator, false);
        log.info("训练完成，耗时：" + (System.currentTimeMillis() - start)/1000 + "(s)");

        model.save();
        Assert.assertTrue(model.exists());
    }

    @org.junit.Test
    public void testUpdateConfig() throws Exception {
        model.load();
        Assert.assertTrue(model.isLoad());

        int featureCount = model.getConfig().getFeatureList().size();

        log.info("当前的均值为：" + model.getConfig().getColMean());
        log.info("当前的标准差为：" + model.getConfig().getColStd());

//        model.getConfig().getColMean().clear();
        Whitebox.invokeMethod(model, "updateConfig"
                , randomDoubleArray(featureCount, -140, -50)
                , randomDoubleArray(featureCount, -1, 1));

        log.info("修改后的均值为：" + model.getConfig().getColMean());
        log.info("修改后的标准差为：" + model.getConfig().getColStd());

//        model.save();
    }

    @org.junit.Test
    public void testStandardlize() throws Exception {
        model.load();

        List<EciMrData> data = loadTrainData("C:\\Users\\DELL\\Desktop\\data\\" + eci + "_0703.txt");
        Assert.assertTrue(data.size() > 0);
        log.info("训练集大小为：" + data.size());
        MrDataSetIterator mrDataSetIterator = new MrDataSetIterator(eci, data, model.getConfig().getFeatureList(), model.getConfig().getLabelList(), TrainingConstant.BATCH_SIZE, TrainingConstant.LEAST_BATCH_SIZE);

        while (mrDataSetIterator.hasNext()){
            mrDataSetIterator.next();
        }

        NormalizerStandardize standardize = (NormalizerStandardize)mrDataSetIterator.getPreProcessor();
        System.out.println("均值：" + standardize.getMean());
        System.out.println("标准差：" + standardize.getStd());
    }

    @org.junit.Test
    public void testPredict() throws Exception {
        model.load();
        Assert.assertTrue(model.isLoad());

        List<EciMrData> dataList = loadTrainData("C:\\Users\\DELL\\Desktop\\data\\" + eci + "_0704.txt");

        final Map<String, Integer> stateCount = new HashMap<>();
        stateCount.put("rightCount", 0);
        stateCount.put("wrongCount", 0);
        Set<Integer> wrongBuildid = new HashSet<>();

        for (EciMrData data : dataList){
            int predictResult = 0;

            if(data.getBuilding() == (predictResult = model.predict(data))){
                stateCount.put("rightCount", stateCount.get("rightCount") + 1);
            }else{
                stateCount.put("wrongCount", stateCount.get("wrongCount") + 1);
                wrongBuildid.add(predictResult);
            }
        }

        log.info("预测正确数【"+ stateCount.get("rightCount") + "】 预测错误数为【" + stateCount.get("wrongCount") + "】");

        log.info("预测错误时的楼宇为：" + wrongBuildid);
    }

    private List<EciMrData> loadTrainData(String dataPath) throws Exception {
        List<EciMrData> data = new ArrayList<>();
        FileReader.readFile(fs, new Path(dataPath), new MrLineParser(data));
        return data;
    }

    private double[] randomDoubleArray(int count, double min, double max){
        int round = (int)(max - min);//获得最大与最小的区间
        Random random = new Random(12354);
        double[] result = new double[count];
        for (int i = 0; i < count; i++){
            int v = random.nextInt(round);
            result[i] = v + min;
            if (result[i] <= 0) {
                result[i] += random.nextDouble();
            } else {
                result[i] -= random.nextDouble();
            }
        }
        return result;
    }

    @org.junit.Test
    public void testRandom() throws Exception {
        double[] a = randomDoubleArray(150, -1, 1);
        for (double aa : a) {
            System.out.println(aa);
        }
    }
}