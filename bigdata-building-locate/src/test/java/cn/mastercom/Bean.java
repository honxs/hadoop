package cn.mastercom;

import cn.mastercom.constants.TrainingConstant;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kwong on 2018/8/30.
 */
public class Bean {

    static class A{

        public void callPrivateMethod(){
            System.out.println(getData(8));
        }

        public void callStaticMethod(){
            System.out.println(B.append("hello", " world!").append(" ni ma bi"));;
        }

        private List<Integer> getData(int size){
            List<Integer> data = new ArrayList<>();

            for (int i = 0; i< size; i++){
                data.add(i);
            }

            return data;
        }
    }
    static class B{
        public static StringBuilder append(String a, String b){
            return new StringBuilder().append(a).append(b);
        }
    }

    static class C{
        public MultiLayerNetwork create(){
            return TrainingConstant.createNetwork(10, 10);
        }
    }
}
