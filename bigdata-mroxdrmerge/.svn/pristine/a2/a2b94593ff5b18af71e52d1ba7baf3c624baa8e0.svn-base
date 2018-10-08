package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class EventHsrTrainSegCellMergeDo implements IMergeDataDo,Serializable {
    private int dataType = 0;
    public StatHsrTrainSegCell statHsrTrainSegCell = new StatHsrTrainSegCell();
    private StringBuffer sbTemp = new StringBuffer();
    @Override
    public String getMapKey() {
        sbTemp.delete(0, sbTemp.length());
        sbTemp.append(statHsrTrainSegCell.iCityID);
        sbTemp.append(statHsrTrainSegCell.lTrainKey);
        sbTemp.append(statHsrTrainSegCell.iSegmentId);
        sbTemp.append(statHsrTrainSegCell.iECI);
        sbTemp.append(statHsrTrainSegCell.iInterface);
        sbTemp.append(statHsrTrainSegCell.kpiSet);
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

        EventHsrTrainSegCellMergeDo data = (EventHsrTrainSegCellMergeDo) o;
        for (int i = 0; i < data.statHsrTrainSegCell.fvalue.length; i++)
        {
            if(data.statHsrTrainSegCell.fvalue[i] > 0)
            {
                if(statHsrTrainSegCell.fvalue[i] < 0){
                    statHsrTrainSegCell.fvalue[i] = data.statHsrTrainSegCell.fvalue[i];
                }else{
                    statHsrTrainSegCell.fvalue[i] += data.statHsrTrainSegCell.fvalue[i];
                }
            }
        }
        return true;
    }

    @Override
    public boolean fillData(String[] vals, int sPos) {
        try {
            statHsrTrainSegCell = StatHsrTrainSegCell.fillData(vals,sPos);
        }catch (Exception e){
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventHsrTrainSegCellMergeDo.fillData " +
                            "error:",
                    "EventHsrTrainSegCellMergeDo.fillData error: " + e.getMessage(),e);
            return false;
        }
        return true;
    }

    @Override
    public String getData() {
        return statHsrTrainSegCell.toString();
    }
}
