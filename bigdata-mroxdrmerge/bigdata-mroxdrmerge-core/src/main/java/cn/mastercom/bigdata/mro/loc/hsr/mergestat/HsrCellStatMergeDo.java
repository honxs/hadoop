package cn.mastercom.bigdata.mro.loc.hsr.mergestat;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mro.loc.hsr.stat.Stat_HsrCell;

import java.io.Serializable;

public class HsrCellStatMergeDo implements IMergeDataDo,Serializable {
    private int dataType = 0;
    Stat_HsrCell stat_hsrCell = new Stat_HsrCell();
    private StringBuffer sbTemp = new StringBuffer();
    @Override
    public String getMapKey() {
        sbTemp.delete(0, sbTemp.length());
        sbTemp.append(stat_hsrCell.iCityID).append("_");
        sbTemp.append(stat_hsrCell.iECI).append("_");
        sbTemp.append(stat_hsrCell.ifreq);
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
        HsrCellStatMergeDo hsrCellStatMergeDo = (HsrCellStatMergeDo) o;
        Stat_HsrCell temp = hsrCellStatMergeDo.stat_hsrCell;
        stat_hsrCell.iMRCnt += temp.iMRCnt;
        stat_hsrCell.iMRRSRQCnt += temp.iMRRSRQCnt;
        stat_hsrCell.iMRSINRCnt += temp.iMRSINRCnt;
        stat_hsrCell.fRSRPValue += temp.fRSRPValue;
        stat_hsrCell.fRSRQValue += temp.fRSRQValue;
        stat_hsrCell.fSINRValue += temp.fSINRValue;
        stat_hsrCell.iMRCnt_95 += temp.iMRCnt_95;
        stat_hsrCell.iMRCnt_100 += temp.iMRCnt_100;
        stat_hsrCell.iMRCnt_103 += temp.iMRCnt_103;
        stat_hsrCell.iMRCnt_105 += temp.iMRCnt_105;
        stat_hsrCell.iMRCnt_110 += temp.iMRCnt_110;
        stat_hsrCell.iMRCnt_113 += temp.iMRCnt_113;
        stat_hsrCell.iMRCnt_128 += temp.iMRCnt_128;
        stat_hsrCell.iRSRP100_SINR0 += temp.iRSRP100_SINR0;
        stat_hsrCell.iRSRP105_SINR0 += temp.iRSRP105_SINR0;
        stat_hsrCell.iRSRP110_SINR3 += temp.iRSRP110_SINR3;
        stat_hsrCell.iRSRP110_SINR0 += temp.iRSRP110_SINR0;
        stat_hsrCell.iSINR_0 += temp.iSINR_0;
        stat_hsrCell.iRSRQ_14 += temp.iRSRQ_14;
        stat_hsrCell.fOverlapTotal += temp.fOverlapTotal;
        stat_hsrCell.iOverlapMRCnt += temp.iOverlapMRCnt;
        stat_hsrCell.fOverlapTotalAll += temp.fOverlapTotalAll;
        stat_hsrCell.iOverlapMRCntAll += temp.iOverlapMRCntAll;
        stat_hsrCell.fRSRPMax += temp.fRSRPMax;
        stat_hsrCell.fRSRPMin += temp.fRSRPMin;
        stat_hsrCell.fRSRQMax += temp.fRSRQMax;
        stat_hsrCell.fRSRQMin += temp.fRSRQMin;
        stat_hsrCell.fSINRMax += temp.fSINRMax;
        stat_hsrCell.fSINRMin += temp.fSINRMin;
        return true;
    }

    @Override
    public boolean fillData(String[] vals, int sPos) {
        try {
            stat_hsrCell = Stat_HsrCell.FillData(vals,0);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public String getData() {
        return stat_hsrCell.toLine();
    }
}
