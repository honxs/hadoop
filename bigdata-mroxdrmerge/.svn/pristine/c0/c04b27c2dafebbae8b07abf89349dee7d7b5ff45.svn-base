package cn.mastercom.bigdata.base.function.impl;

import cn.mastercom.bigdata.base.function.AbstractFunction;
import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.base.model.ExternalDO;
import cn.mastercom.bigdata.util.DataAdapterReader;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * Created by Kwong on 2018/7/13.
 */
public class StringToExternalDOFunction<O extends ExternalDO> extends AbstractFunction<String, O> {

    DataAdapterReader dataAdapterReader;

    Class<O> clazz;

    public StringToExternalDOFunction(DataAdapterReader dataAdapterReader){

        Objects.requireNonNull(dataAdapterReader);
        this.dataAdapterReader = dataAdapterReader;

        try{
            Type type = StringToExternalDOFunction.class.getGenericSuperclass();
            Type[] types = ((ParameterizedType) type).getActualTypeArguments();
            clazz = (Class<O>) types[2];
        }catch (Exception e){
            throw new ClassFormatError("没有找到合适的范型类型");
        }

    }

    @Override
    public O apply(String value) {
        O result = null;
        try {
            result = (O)clazz.newInstance();
            if (result != null){
                result.fillData(dataAdapterReader);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
