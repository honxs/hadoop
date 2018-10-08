package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.ResultOutputer;

public class MrResidentSampleStatDo_4G extends MrSampleStatDo_4G {

    public MrResidentSampleStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
    {
        super(typeResult, sourceType, dataType);
    }

    @Override
    protected boolean statOrNot(DT_Sample_4G sample)
    {
        return sample.Eci > 0 && sample.isResidentUser && Func.getDateType(sample.itime) != StaticConfig.OTHERTIME;
    }
}
