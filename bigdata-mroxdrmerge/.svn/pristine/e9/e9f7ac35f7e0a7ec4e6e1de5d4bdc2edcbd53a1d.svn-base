package cn.mastercom.bigdata.base.model.impl;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.Stat;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 抽象统计
 * 作用：将 泛型 O 转化为 维度 和 统计
 * Created by Kwong on 2018/7/16.
 */
public abstract class AbstractStat<O> implements Stat<O> {

    protected AbstractDimension dimension;

    protected AbstractAggregator aggregator;


    /**
     * 统计维度的个数
     */
    int dimensionLength;


    @Override
    public void fromFormatedString(String value) throws Exception {

//        if(dimensionLength <= 0)
//            dimensionLength = dimension.dimensionList.size();

 /*       String[] values = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR, -1);

        String[] dimensions = Arrays.copyOf(values, dimensionLength);
        String[] aggregators = Arrays.copyOfRange(values, dimensionLength, values.length);

        dimension.fromFormatedStrArr(dimensions);
        aggregator.fromFormatedStrArr(aggregators);*/
    }

    @Override
    public String toFormatedString() {

        return dimension.toFormatedString() + DataConstant.DEFAULT_COLUMN_SEPARATOR + aggregator.toFormatedString();

    }

    public abstract static class AbstractDimension implements Dimension {

        /**
         * 不允许有 除String外的 非基础类型
         */
        protected Field[] fields = getClass().getDeclaredFields();

        public AbstractDimension(){}

        public Field[] getFields() {
            return fields;
        }

        @Override
        public int hashCode(){
            Object[] values = new Object[fields.length];
            for (int i = 0 ; i < fields.length; i++){
                try {
                     values[i] = fields[i].get(this);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            return Objects.hash(values);
        }

        @Override
        public boolean equals(Object obj){
            if(obj.getClass() == getClass()){
                AbstractDimension that = (AbstractDimension)obj;
                boolean isEqual = true;
                if (that.fields.length == this.fields.length){
                    for (int i = 0; i< this.fields.length;i++){
                        if (!this.fields[i].equals(that.fields[i])){
                            isEqual = false;
                            break;
                        }
                    }
                    return isEqual;
                }
            }
            return false;

        }

        @Override
        public String toString() {
            return toFormatedString();
        }
    }

    public abstract static class AbstractAggregator implements Aggregator {

        @Override
        public void fromFormatedString(String value) throws Exception {
            fromFormatedStrArr(value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR));
        }

        public abstract void fromFormatedStrArr(String[] values) throws Exception;
    }
}
