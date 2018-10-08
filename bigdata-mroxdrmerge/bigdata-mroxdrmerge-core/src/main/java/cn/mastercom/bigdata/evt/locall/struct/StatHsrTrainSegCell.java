package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class StatHsrTrainSegCell implements Serializable {
    public int  iCityID;
    public long lTrainKey;
    public int iSegmentId;
    public long iECI;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    public double fvalue[] = new double[20];
    public static final String spliter = "\t";

    public static StatHsrTrainSegCell fillData(String[] vals, int pos){
        StatHsrTrainSegCell statHsrTrainSegCell = new StatHsrTrainSegCell();
        try {
            statHsrTrainSegCell.iCityID = Integer.parseInt(vals[pos++]);
            statHsrTrainSegCell.lTrainKey = Long.parseLong(vals[pos++]);
            statHsrTrainSegCell.iSegmentId = Integer.parseInt(vals[pos++]);
            statHsrTrainSegCell.iECI = Integer.parseInt(vals[pos++]);
            statHsrTrainSegCell.iInterface = Integer.parseInt(vals[pos++]);
            statHsrTrainSegCell.kpiSet = Integer.parseInt(vals[pos++]);
            statHsrTrainSegCell.iTime = Integer.parseInt(vals[pos++]);
            for (int i = 0; i < statHsrTrainSegCell.fvalue.length; i++)
            {
                statHsrTrainSegCell.fvalue[i] = Double.parseDouble(vals[pos++]);
            }

        }catch (Exception e){
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"StatHsrTrainSegCell.fillData error",
                    "StatHsrTrainSegCell.fillData error: " + e.getMessage(),e);
        }
        return statHsrTrainSegCell;

    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(iCityID).append(spliter);
        sb.append(lTrainKey).append(spliter);
        sb.append(iSegmentId).append(spliter);
        sb.append(iECI).append(spliter);
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
