package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by Kwong on 2018/7/16.
 */
public class DelegateDimension  extends AbstractStat.AbstractDimension {

    private List<Stat.Dimension> dimensionList;

    /**
     * 将dimensionList按这个顺序排序
     */
    private Class[] dimensionClassOrders;

    public DelegateDimension(Stat.Dimension... dimensions){
        //默认顺序： cityId  grid    eci freq    time
        this(new Class[]{CityDimension.class, XdrInterfaceDimension.class, GridDimension.class, CellDimension.class, FreqDimension.class, TimeDimension.class}, dimensions);
    }

    public DelegateDimension(Class[] dimensionClassOrders, Stat.Dimension... dimensions){

        dimensionList = new ArrayList<>();

        for(Stat.Dimension dimension : dimensions){
            if (dimension != null){
                if (dimension instanceof DelegateDimension){

                    dimensionList.addAll(((DelegateDimension)dimension).getDimensionList());

                }else{

                    dimensionList.add(dimension);
                }
            }
        }
        sort(dimensionList, dimensionClassOrders);

        initFields();
    }

    private void initFields(){
        List<Field> fieldList = new ArrayList<>();
        for(Stat.Dimension dimension : dimensionList){
            fieldList.addAll(Arrays.asList(((AbstractStat.AbstractDimension)dimension).getFields()));
        }
        fields = fieldList.toArray(new Field[0]);
    }

    public List<Stat.Dimension> getDimensionList() {
        return dimensionList;
    }


    @Override
    public int hashCode() {
        return Objects.hash(dimensionList.toArray());
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof DelegateDimension){

            DelegateDimension dimension = (DelegateDimension)obj;
            if (this.dimensionList.size() == dimension.getDimensionList().size()){

                List<Stat.Dimension> otherDimensionList = dimension.getDimensionList();
                boolean isEqual = true;
                for (int i = 0; i< dimensionList.size();i++){
                    if (!dimensionList.get(i).equals(otherDimensionList.get(i))){
                        isEqual = false;
                        break;
                    }
                }
                return isEqual;
            }

        }else if(dimensionList.size() == 1 && dimensionList.get(0).equals(obj)){

            return true;
        }
        return false;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {

        for(Stat.Dimension dimension : dimensionList){

            int length = ((AbstractStat.AbstractDimension)dimension).getFields().length;
            int index = -1;
            while(length > 0){

                index = value.indexOf(DataConstant.DEFAULT_COLUMN_SEPARATOR, index + 1);
                length--;
            }
            if (index > 0){

                dimension.fromFormatedString(value.substring(0, index));
                //去掉一个\t
                value = value.substring(index + 1);

            }else {

                dimension.fromFormatedString(value);
            }


        }
    }

    @Override
    public String toFormatedString() {

        StringBuilder sb = new StringBuilder();

        for(Stat.Dimension dimension : dimensionList){

            sb.append(dimension.toFormatedString()).append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

        }
        if (sb.length() > 0) return sb.substring(0, sb.lastIndexOf(DataConstant.DEFAULT_COLUMN_SEPARATOR));
        else return "";
    }

    /**
     * 返回dimensionList所有包含的维度
     * @return
     */
    @Override
    public Field[] getFields() {
        return super.getFields();
    }

    void sort(List<Stat.Dimension> d, final Class[] value){
        if(d != null){
            Collections.sort(d,new Comparator<Stat.Dimension>() {
                public int compare(Stat.Dimension d1,Stat.Dimension d2) {
                    int i1 = indexOf(value, d1.getClass());
                    int i2 = indexOf(value, d2.getClass());
                    return i1-i2;
                }
            });
        }
    }

    int indexOf(Class[] f, Class o){
        if(o != null){
            for(int i=0;i<f.length;i++){
                if(f[i] == o){
                    return i;
                }
            }
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {

//        String value = "1101\t19940865\t1516863600\t55\t55\t55\t-5166.0\t-497.0\t851.0\t18\t42\t54\t55\t55\t55\t55\t40\t53\t51\t53\t53\t55\t102.0\t0\t142.0\t16\t-74.0\t-104.0\t-6.0\t-14.0\t25.0\t-2.0";

//        DelegateDimension delegateDimension = new DelegateDimension(new CellDimension(), new CityDimension(), new TimeDimension());

        String value = "1101\t1163270000\t398102400\t1163272000\t398100600\t23994117\t1516863600\t8\t8\t8\t-722.0\t-84.0\t138.0\t8\t8\t8\t8\t8\t8\t8\t8\t8\t8\t8\t8\t8\t17.0\t0\t35.0\t4\t-88.0\t-92.0\t-9.0\t-13.0\t22.0\t15.0";

        DelegateDimension delegateDimension = new DelegateDimension(new CellDimension(), new CityDimension(), new TimeDimension(), new GridDimension());

        delegateDimension.fromFormatedString(value);

        for(Stat.Dimension dimension : delegateDimension.getDimensionList()){
            System.out.println(dimension.getClass());
            System.out.println(dimension.toFormatedString());
            System.out.println("-----------------");
        }
    }
}
