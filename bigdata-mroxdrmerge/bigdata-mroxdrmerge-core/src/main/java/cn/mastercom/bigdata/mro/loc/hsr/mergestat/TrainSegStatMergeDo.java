package cn.mastercom.bigdata.mro.loc.hsr.mergestat;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mro.loc.hsr.stat.Stat_TrainSeg;

import java.io.Serializable;

public class TrainSegStatMergeDo implements IMergeDataDo,Serializable {
    private int dataType = 0;
    Stat_TrainSeg statTrainSeg = new Stat_TrainSeg();
    private StringBuffer sbTemp = new StringBuffer();
    @Override
    public String getMapKey() {
        sbTemp.delete(0, sbTemp.length());
        sbTemp.append(statTrainSeg.iCityID).append("_");
        sbTemp.append(statTrainSeg.lTrainKey).append("_");
        sbTemp.append(statTrainSeg.iSegmentId);
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
        TrainSegStatMergeDo trainSegStatMergeDo = (TrainSegStatMergeDo) o;
        Stat_TrainSeg temp = trainSegStatMergeDo.statTrainSeg;
        statTrainSeg.iMRCnt += temp.iMRCnt;
        statTrainSeg.iMRRSRQCnt += temp.iMRRSRQCnt;
        statTrainSeg.iMRSINRCnt += temp.iMRSINRCnt;
        statTrainSeg.fRSRPValue += temp.fRSRPValue;
        statTrainSeg.fRSRQValue += temp.fRSRQValue;
        statTrainSeg.fSINRValue += temp.fSINRValue;
        statTrainSeg.iMRCnt_95 += temp.iMRCnt_95;
        statTrainSeg.iMRCnt_100 += temp.iMRCnt_100;
        statTrainSeg.iMRCnt_103 += temp.iMRCnt_103;
        statTrainSeg.iMRCnt_105 += temp.iMRCnt_105;
        statTrainSeg.iMRCnt_110 += temp.iMRCnt_110;
        statTrainSeg.iMRCnt_113 += temp.iMRCnt_113;
        statTrainSeg.iMRCnt_128 += temp.iMRCnt_128;
        statTrainSeg.iRSRP100_SINR0 += temp.iRSRP100_SINR0;
        statTrainSeg.iRSRP105_SINR0 += temp.iRSRP105_SINR0;
        statTrainSeg.iRSRP110_SINR3 += temp.iRSRP110_SINR3;
        statTrainSeg.iRSRP110_SINR0 += temp.iRSRP110_SINR0;
        statTrainSeg.iSINR_0 += temp.iSINR_0;
        statTrainSeg.iRSRQ_14 += temp.iRSRQ_14;
        statTrainSeg.fOverlapTotal += temp.fOverlapTotal;
        statTrainSeg.iOverlapMRCnt += temp.iOverlapMRCnt;
        statTrainSeg.fOverlapTotalAll += temp.fOverlapTotalAll;
        statTrainSeg.iOverlapMRCntAll += temp.iOverlapMRCntAll;
        statTrainSeg.fRSRPMax += temp.fRSRPMax;
        statTrainSeg.fRSRPMin += temp.fRSRPMin;
        statTrainSeg.fRSRQMax += temp.fRSRQMax;
        statTrainSeg.fRSRQMin += temp.fRSRQMin;
        statTrainSeg.fSINRMax += temp.fSINRMax;
        statTrainSeg.fSINRMin += temp.fSINRMin;
        return true;
    }

    @Override
    public boolean fillData(String[] vals, int sPos) {
        try{
            statTrainSeg = Stat_TrainSeg.FillData(vals,sPos);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public String getData() {
        return statTrainSeg.toLine();
    }
}
