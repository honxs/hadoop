package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class StatHsrTrainSeg implements Serializable {
    public int  iCityID;
    public long lTrainKey;
    public int iSegmentId;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    public double fvalue[] = new double[20];
    public static final String spliter = "\t";

    public static StatHsrTrainSeg fillData(String[] vals, int pos){
        StatHsrTrainSeg statHsrTrainSeg = new StatHsrTrainSeg();
        try {
            statHsrTrainSeg.iCityID = Integer.parseInt(vals[pos++]);
            statHsrTrainSeg.lTrainKey = Long.parseLong(vals[pos++]);
            statHsrTrainSeg.iSegmentId = Integer.parseInt(vals[pos++]);
            statHsrTrainSeg.iInterface = Integer.parseInt(vals[pos++]);
            statHsrTrainSeg.kpiSet = Integer.parseInt(vals[pos++]);
            statHsrTrainSeg.iTime = Integer.parseInt(vals[pos++]);
            for (int i = 0; i < statHsrTrainSeg.fvalue.length; i++)
            {
                statHsrTrainSeg.fvalue[i] = Double.parseDouble(vals[pos++]);
            }

        }catch (Exception e){
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"StatHsrTrainSeg.fillData error",
                    "StatHsrTrainSeg.fillData error: " + e.getMessage(),e);
        }
        return statHsrTrainSeg;

    }
    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(iCityID).append(spliter);
        sb.append(lTrainKey).append(spliter);
        sb.append(iSegmentId).append(spliter);
        sb.append(iInterface).append(spliter);
        sb.append(kpiSet).append(spliter);
        sb.append(iTime).append(spliter);
        for (int i = 0; i < fvalue.length; i++)
        {
            sb.append(fvalue[i]);
            if(i!=fvalue.length-1){
                sb.append(spliter);
            }
        }
        return sb.toString();
    }
}
