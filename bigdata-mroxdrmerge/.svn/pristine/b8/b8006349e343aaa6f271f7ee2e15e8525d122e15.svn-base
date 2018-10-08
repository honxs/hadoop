package cn.mastercom.bigdata.stat.aggregate.aggregator;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.stat.aggregate.value.MRAggregateValue;

/**
 * Created by Kwong on 2018/7/16.
 */
public class MRAggregator implements Stat.Aggregator<MRAggregateValue>, Stat.Aggregator.Values{

    protected int iMRCnt;
    protected int iMRRSRQCnt;
    protected int iMRSINRCnt;
    protected double fRSRPValue;
    protected double fRSRQValue;
    protected double fSINRValue;
    protected int iMRCnt_95;
    protected int iMRCnt_100;
    protected int iMRCnt_103;
    protected int iMRCnt_105;
    protected int iMRCnt_110;
    protected int iMRCnt_113;
    protected int iMRCnt_128;
    protected int iRSRP100_SINR0;
    protected int iRSRP105_SINR0;
    protected int iRSRP110_SINR3;
    protected int iRSRP110_SINR0;
    protected int iSINR_0;
    protected int iRSRQ_14;
    protected float fOverlapTotal;
    protected int iOverlapMRCnt;
    protected float fOverlapTotalAll;
    protected int iOverlapMRCntAll;
    protected float fRSRPMax = StaticConfig.Int_Abnormal;
    protected float fRSRPMin = StaticConfig.Int_Abnormal;
    protected float fRSRQMax = StaticConfig.Int_Abnormal;
    protected float fRSRQMin = StaticConfig.Int_Abnormal;
    protected float fSINRMax = StaticConfig.Int_Abnormal;
    protected float fSINRMin = StaticConfig.Int_Abnormal;
    
    @Override
    public void fromFormatedString(String value) throws Exception {

    }

    @Override
    public String toFormatedString() {

        StringBuffer bf = new StringBuffer();
        bf.append(iMRCnt);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRRSRQCnt);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRSINRCnt);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fRSRPValue);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fRSRQValue);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fSINRValue);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_95);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_100);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_103);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_105);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_110);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_113);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iMRCnt_128);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iRSRP100_SINR0);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iRSRP105_SINR0);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iRSRP110_SINR3);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iRSRP110_SINR0);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iSINR_0);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iRSRQ_14);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fOverlapTotal);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iOverlapMRCnt);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fOverlapTotalAll);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(iOverlapMRCntAll);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fRSRPMax);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fRSRPMin);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fRSRQMax);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fRSRQMin);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fSINRMax);
        bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
        bf.append(fSINRMin);
        return bf.toString();
    }

    @Override
    public void apply(MRAggregateValue value) {
        if (!(value.rsrp >= -150 && value.rsrp <= -30))
            return;
        iMRCnt++;
        fRSRPValue += value.rsrp; // 总的MR的RSRP值，按服务小区计算

        if (value.rsrq > -10000)
        {
            iMRRSRQCnt++;
            fRSRQValue += value.rsrq;
        }
        if (value.sinr >= -1000 && value.sinr <= 1000) // 主服上行干扰比
        {
            iMRSINRCnt++;
            fSINRValue += value.sinr; // 总的MR的RSRQ值，按服务小区计算
        }
        if (value.rsrp >= -95)
        {
            iMRCnt_95++; // 大于等于-95dB的采样点数，按服务小区计算
        }
        if (value.rsrp >= -100)
        {
            iMRCnt_100++;
        }
        if (value.rsrp >= -103)
        {
            iMRCnt_103++;
        }
        if (value.rsrp >= -105)
        {
            iMRCnt_105++;
        }
        if (value.rsrp >= -110)
        {
            iMRCnt_110++;
        }
        if (value.rsrp >= -113)
        {
            iMRCnt_113++;
        }
        if (value.rsrp >= -128)
        {
            iMRCnt_128++;
        }
        // rsrp_sinr
        if ((value.rsrp >= -100) && (value.sinr >= 0))
        {
            iRSRP100_SINR0++;

        }
        if ((value.rsrp >= -105) && (value.sinr >= 0))
        {
            iRSRP105_SINR0++;

        }
        if ((value.rsrp >= -110) && (value.sinr >= 3))
        {
            iRSRP110_SINR3++;

        }
        if ((value.rsrp >= -110) && (value.sinr >= 0))
        {
            iRSRP110_SINR0++;
        }
        if (value.sinr >= 0)
        {
            iSINR_0++;
        }
        if (value.rsrq >= -14)
        {
            iRSRQ_14++;
        }

        fOverlapTotal += value.overLap;
        if (value.overLap >= 4)
        {
            iOverlapMRCnt++;
        }
        fOverlapTotalAll += value.overLapAll;
        if (value.overLapAll >= 4)
        {
            iOverlapMRCntAll++;
        }

        fRSRPMax = getMax(fRSRPMax, value.rsrp);
        fRSRPMin = getMin(fRSRPMin, value.rsrp);
        fRSRQMax = getMax(fRSRQMax, value.rsrq);
        fRSRQMin = getMin(fRSRQMin, value.rsrq);
        fSINRMax = getMax(fSINRMax, value.sinr);
        fSINRMin = getMin(fSINRMin, value.sinr);
    }

    private float getMax(float valueMax, int value)
    {
        if (valueMax == StaticConfig.Int_Abnormal || valueMax < value)
        {
            return value;
        }
        return valueMax;
    }

    private float getMin(float valueMin, int value)
    {
        if (value == StaticConfig.Int_Abnormal)
            return valueMin;
        if (valueMin == StaticConfig.Int_Abnormal || valueMin > value)
        {
            return value;
        }
        return valueMin;
    }

    @Override
    public String toString() {
        return toFormatedString();
    }
}
