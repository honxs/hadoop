package cn.mastercom.exception;

/**
 * Created by Kwong on 2018/8/14.
 */
public class ModelNotTrainedException extends Exception{

    public ModelNotTrainedException(){ super();}

    public ModelNotTrainedException(String msg){
        super(msg);
    }
}
