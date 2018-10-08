package cn.mastercom.bigdata.mro.loc.hsr.mergestat;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mro.loc.hsr.stat.Stat_TrainSegCell;

import java.io.Serializable;

public class TrainSegCellStatMergeDo implements IMergeDataDo,Serializable {
    private int dataType = 0;
    Stat_TrainSegCell statTrainSegCell = new Stat_TrainSegCell();
    private StringBuffer sbTemp = new StringBuffer();

    @Override
    public String getMapKey() {
        sbTemp.delete(0, sbTemp.length());
        sbTemp.append(statTrainSegCell.iCityID).append("_");
        sbTemp.append(statTrainSegCell.lTrainKey).append("_");
        sbTemp.append(statTrainSegCell.iSegmentId).append("_");
        sbTemp.append(statTrainSegCell.lEci);
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
        TrainSegCellStatMergeDo trainSegCellStatMergeDo = (TrainSegCellStatMergeDo) o;
        Stat_TrainSegCell temp = trainSegCellStatMergeDo.statTrainSegCell ;
        statTrainSegCell.iMRCnt += temp.iMRCnt;
        statTrainSegCell.iMRRSRQCnt += temp.iMRRSRQCnt;
        statTrainSegCell.iMRSINRCnt += temp.iMRSINRCnt;
        statTrainSegCell.fRSRPValue += temp.fRSRPValue;
        statTrainSegCell.fRSRQValue += temp.fRSRQValue;
        statTrainSegCell.fSINRValue += temp.fSINRValue;
        statTrainSegCell.iMRCnt_95 += temp.iMRCnt_95;
        statTrainSegCell.iMRCnt_100 += temp.iMRCnt_100;
        statTrainSegCell.iMRCnt_103 += temp.iMRCnt_103;
        statTrainSegCell.iMRCnt_105 += temp.iMRCnt_105;
        statTrainSegCell.iMRCnt_110 += temp.iMRCnt_110;
        statTrainSegCell.iMRCnt_113 += temp.iMRCnt_113;
        statTrainSegCell.iMRCnt_128 += temp.iMRCnt_128;
        statTrainSegCell.iRSRP100_SINR0 += temp.iRSRP100_SINR0;
        statTrainSegCell.iRSRP105_SINR0 += temp.iRSRP105_SINR0;
        statTrainSegCell.iRSRP110_SINR3 += temp.iRSRP110_SINR3;
        statTrainSegCell.iRSRP110_SINR0 += temp.iRSRP110_SINR0;
        statTrainSegCell.iSINR_0 += temp.iSINR_0;
        statTrainSegCell.iRSRQ_14 += temp.iRSRQ_14;
        statTrainSegCell.fOverlapTotal += temp.fOverlapTotal;
        statTrainSegCell.iOverlapMRCnt += temp.iOverlapMRCnt;
        statTrainSegCell.fOverlapTotalAll += temp.fOverlapTotalAll;
        statTrainSegCell.iOverlapMRCntAll += temp.iOverlapMRCntAll;
        statTrainSegCell.fRSRPMax += temp.fRSRPMax;
        statTrainSegCell.fRSRPMin += temp.fRSRPMin;
        statTrainSegCell.fRSRQMax += temp.fRSRQMax;
        statTrainSegCell.fRSRQMin += temp.fRSRQMin;
        statTrainSegCell.fSINRMax += temp.fSINRMax;
        statTrainSegCell.fSINRMin += temp.fSINRMin;

        return true;
    }

    @Override
    public boolean fillData(String[] vals, int sPos) {
        try {
            statTrainSegCell = Stat_TrainSegCell.FillData(vals,sPos);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public String getData() {
        return statTrainSegCell.toLine();
    }
}
