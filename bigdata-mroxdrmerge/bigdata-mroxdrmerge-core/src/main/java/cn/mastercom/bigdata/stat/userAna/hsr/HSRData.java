package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.stat.userAna.model.Xdr_ImsiEciTime;

public class HSRData
{
    public int hourTime;
    public List<Xdr_ImsiEciTime> xdrLocationList;
    public List<HSRSecTrainData> secTrainDataList;

    public HSRData()
    {
        xdrLocationList = new ArrayList<Xdr_ImsiEciTime>();
        secTrainDataList = new ArrayList<HSRSecTrainData>();
    }

}
