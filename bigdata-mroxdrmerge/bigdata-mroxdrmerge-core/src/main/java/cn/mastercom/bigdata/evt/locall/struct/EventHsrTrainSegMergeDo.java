package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class EventHsrTrainSegMergeDo implements IMergeDataDo,Serializable {
    private int dataType = 0;
    public StatHsrTrainSeg statHsrTrainSeg = new StatHsrTrainSeg();
    private StringBuffer sbTemp = new StringBuffer();
    @Override
    public String getMapKey() {
        sbTemp.delete(0, sbTemp.length());
        sbTemp.append(statHsrTrainSeg.iCityID);
        sbTemp.append(statHsrTrainSeg.lTrainKey);
        sbTemp.append(statHsrTrainSeg.iSegmentId);
        sbTemp.append(statHsrTrainSeg.iInterface);
        sbTemp.append(statHsrTrainSeg.kpiSet);
        return sbTemp.toString();
    }

    @Override
    public int getDataType() {
        return dataType;
    }

    @Override
    public int setDataType(int dataType) {
        this.dataType = dataType;
        return 0;
    }

    @Override
    public boolean mergeData(Object o) {

        EventHsrTrainSegMergeDo data = (EventHsrTrainSegMergeDo) o;
        for (int i = 0; i < data.statHsrTrainSeg.fvalue.length; i++)
        {
            if(data.statHsrTrainSeg.fvalue[i] > 0)
            {
                if(statHsrTrainSeg.fvalue[i] < 0){
                    statHsrTrainSeg.fvalue[i] = data.statHsrTrainSeg.fvalue[i];
                }else{
                    statHsrTrainSeg.fvalue[i] += data.statHsrTrainSeg.fvalue[i];
                }
            }
        }
        return true;
    }

    @Override
    public boolean fillData(String[] vals, int sPos) {
        try {
            statHsrTrainSeg = StatHsrTrainSeg.fillData(vals,sPos);
        }catch (Exception e){
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventHsrTrainSegMergeDo.fillData error",
                    "EventHsrTrainSegMergeDo.fillData error: " + e.getMessage(),e);
            return false;
        }
        return true;
    }

    @Override
    public String getData() {
        return statHsrTrainSeg.toString();
    }
}
