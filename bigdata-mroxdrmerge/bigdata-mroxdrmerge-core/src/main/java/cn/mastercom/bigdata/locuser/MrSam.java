package cn.mastercom.bigdata.locuser;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;

public class MrSam
{
    public int itime = 0; // 聚合时间点
    public int cityid = 0;
	public int eci = 0;	
	public long MmeUeS1apId = 0;
	public String mrotype = "";
	public Map<Integer, Mrcell> cells = new HashMap<Integer, Mrcell>();
	
	public SIGNAL_MR_All mall = null;
}