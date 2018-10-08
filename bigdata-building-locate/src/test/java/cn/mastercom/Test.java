package cn.mastercom;

import cn.mastercom.constants.TrainingConstant;
import cn.mastercom.exception.ModelNotTrainedException;
import cn.mastercom.locating.mr.EciClassifyModel;
import cn.mastercom.locating.mr.EciMrData;
import cn.mastercom.util.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.api.storage.StatsStorage;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.api.IterationListener;
import org.deeplearning4j.ui.api.UIServer;
import org.deeplearning4j.ui.stats.StatsListener;
import org.deeplearning4j.ui.storage.InMemoryStatsStorage;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.PrepareOnlyThisForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.Tuple2;
import scala.Tuple5;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import static org.powermock.api.mockito.PowerMockito.*;

import java.io.*;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static cn.mastercom.constants.TrainingConstant.*;
import static cn.mastercom.constants.TrainingConstant.NUM_HIDDEN_NODE2;
import static cn.mastercom.util.ValueUtils.INVALID_INT_VALUE;
import static cn.mastercom.util.ValueUtils.getValidInt;
import static cn.mastercom.Bean.*;

/**
 * Created by Kwong on 2018/8/6.
 */
@RunWith(PowerMockRunner.class)
@PrepareOnlyThisForTest({/*A.class, */B.class, TrainingConstant.class})//@Prepare会默认将当前类中所有的class包括内部类嵌套类也prepare了
@PowerMockIgnore({"java.*", "javax.*", "org.*", "akka.*", "scala.*", "play.*"})//第三方包全都不要mock，不然会出现意想不到的事情
public class Test {

   /* @Before
    public void setup(){
        System.setProperty("HADOOP_USER_NAME", "mastercom");
    }*/

    @Ignore
   @org.junit.Test
    public void testPredict() throws FileNotFoundException {

       final MrLocationPredict service = new MrLocationPredict("E:\\mt_wlyh\\Data\\config\\model\\");

       final Map<String, Integer> stateCount = new HashMap<>();
       stateCount.put("rightCount", 0);
       stateCount.put("wrongCount", 0);

       final Set<Integer> wrongBuildid = new HashSet<>();

       FileUtils.readFileByLine("E:\\文件\\MR定位楼宇-神经网络\\data\\127753474_0704.txt", new FileUtils.LineHandler() {

           @Override
           public void handle(String line) {

               String[] words = line.split("\t", -1);

               if(words.length < 100) return;

               EciMrData mrData = new EciMrData(words[8], getValidInt(words[3]), getValidInt(words[4]), getValidInt(words[14]), getValidInt(words[15]), getValidInt(words[18]), getValidInt(words[20]), getValidInt(words[21]), getValidInt(words[22]), getValidInt(words[24]), getValidInt(words[25]), getValidInt(words[27]), getValidInt(words[28]), getValidInt(words[29]), getValidInt(words[30]), getValidInt(words[32]), getValidInt(words[33]), getValidInt(words[34]), getValidInt(words[35]), getValidInt(words[37]), getValidInt(words[38]), getValidInt(words[39]), getValidInt(words[40]), getValidInt(words[42]), getValidInt(words[43]), getValidInt(words[44]), getValidInt(words[45]), getValidInt(words[47]), getValidInt(words[48]), getValidInt(words[49]), getValidInt(words[50]), getValidInt(words[52]), getValidInt(words[53]), getValidInt(words[54]), getValidInt(words[55]) );

               int predictResult = 0;

               if(mrData.getBuilding() == (predictResult = service.predict(mrData))){
                   stateCount.put("rightCount", stateCount.get("rightCount") + 1);
               }else{
                   stateCount.put("wrongCount", stateCount.get("wrongCount") + 1);
                   wrongBuildid.add(predictResult);
               }

           }
       });
       System.out.println("预测正确数【"+ stateCount.get("rightCount") + "】 预测错误数为【" + stateCount.get("wrongCount") + "】");

       System.out.println("预测错误时的楼宇为：" + wrongBuildid);


   }

    @org.junit.Test
    public void train() throws Exception {

       String[] args = new String[]{"E:\\文件\\MR定位楼宇-神经网络\\data\\127753474_0703.txt", "E:\\mt_wlyh\\Data\\config\\model\\", "true"};

       SparkConf sparkConf = new SparkConf();
       sparkConf.setAppName("mrBuildCalculate");
        sparkConf.setMaster("local[1]");

       SparkContext sc = new SparkContext(sparkConf);

       try (JavaSparkContext jsc = new JavaSparkContext(sc)){

           UIServer uiServer = UIServer.getInstance();
           final StatsStorage statsStorage = new InMemoryStatsStorage();
           uiServer.attach(statsStorage);

           mockStatic(TrainingConstant.class);


           when(TrainingConstant.getFeatureMarkNcRsrp(Mockito.anyInt(), Mockito.anyInt())).thenCallRealMethod();
           when(TrainingConstant.getFeatureMarkNcRsrq(Mockito.anyInt(), Mockito.anyInt())).thenCallRealMethod();
           when(TrainingConstant.createNetwork(Mockito.anyInt(), Mockito.anyInt())).thenAnswer(new Answer<MultiLayerNetwork>() {
               @Override
               public MultiLayerNetwork answer(InvocationOnMock invocationOnMock) throws Throwable {
                   System.out.println("===========================================================");
                   MultiLayerNetwork result = (MultiLayerNetwork)invocationOnMock.callRealMethod();
                   System.out.println("===========================================================");
                   result.setListeners(new StatsListener(statsStorage));
                   return result;
               }
           });
           MrLocationServiceOnSparkImpl service = new MrLocationServiceOnSparkImpl(jsc, args);
//           service.train();
           service.testCreateNetwork();
       }

       sc.stop();
//        C c = new C();
//        c.create();
        /*new Thread(new Runnable() {
            @Override
            public void run() {
                MultiLayerNetwork network = TrainingConstant.createNetwork(10, 10);
                Collection<IterationListener> listeners = network.getListeners();
                System.out.println("listeners.size = " + listeners.size());
                for (IterationListener listener : listeners){
                    System.out.println(listener.toString());
                }
            }
        }).start();

        Thread.currentThread().sleep(5000);*/
   }

    @org.junit.Test
    public void testMockPrivateMethod() throws Exception {
       A a = new A();
       final A mock = PowerMockito.spy(a);
       PowerMockito.when(mock, "getData", Mockito.anyInt())
                .thenAnswer(new Answer<List<Integer>>() {
                    @Override
                    public List<Integer> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        List<Integer> result = (List<Integer>)invocationOnMock.callRealMethod();
                        result.add(123456789);
                        return result;
                    }
                });

        mock.callPrivateMethod();
    }

    @org.junit.Test
    public void testMockStaticMethod() throws Exception {
        mockStatic(B.class);
        when(B.append(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(new Answer<StringBuilder>() {
                    @Override
                    public StringBuilder answer(InvocationOnMock invocationOnMock) throws Throwable {
                        StringBuilder result = (StringBuilder)invocationOnMock.callRealMethod();
                        result.append(" 强行插入 ");
                        return result;
                    }
                });
        A a = new A();
        a.callStaticMethod();
    }


    @org.junit.Test
    public void testAuth() throws ClassNotFoundException, IOException {
        //以下代码使用@Runwith(Powermock.class)会报错，报错原因定位到jaas的Subject类，可能是由于powermock的自定义classloader影响的

        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

        System.out.println(ugi.getShortUserName());
    }

    @org.junit.Test
    public void testStatic(){
        mockStatic(TrainingConstant.class);

        System.out.println(TrainingConstant.FEATURE_MARK_NC_COUNT);
    }


}
