package cn.mastercom.bigdata.stat.aggregate.value;

import cn.mastercom.bigdata.base.model.Stat;

/**
 * Created by Kwong on 2018/7/16.
 */
public class MRAggregateValue implements Stat.Aggregator.Values{
    
    public final int rsrp;

    public final int rsrq;

    public final int sinr;

    public final int overLap;

    public final int overLapAll;

    public MRAggregateValue(int lteScRSRP, int lteScRSRQ, int lteScSinrUL,  int overlapSameEarfcn, int overlapAll) {
        rsrp = lteScRSRP;
        rsrq = lteScRSRQ;
        sinr = lteScSinrUL;
        overLap = overlapSameEarfcn;
        overLapAll = overlapAll;
    }
}
