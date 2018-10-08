package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;

public class TopicCellIsolated implements IResultTable
{
	public int iCityID = 0;
	public int iECI = 0;
	public int iTime = 0;
	public int iMRCnt = 0;
	public int iMRCnt_NoNei = 0;
	public int iMRCnt_Nei = 0;
	public int iMRCnt_Nei_95 = 0;
	public int iMRCnt_Nei_100 = 0;
	public int iMRCnt_Nei_103 = 0;
	public int iMRCnt_Nei_105 = 0;
	public int iMRCnt_Nei_110 = 0;
	public int iMRCnt_Nei_113 = 0;
	
	public TopicCellIsolated(DT_Sample_4G sample, int statTime)
	{
		iCityID = sample.cityID;
		iECI = (int)sample.Eci;
		iTime = statTime;
	}
	
	public static final String TypeName = "topiccellisolated";
	
	public static String getKey(DT_Sample_4G sample)
	{
		//此处按天计算,所以没有添加时间因素
		return sample.cityID 
				+ "_" + sample.Eci;
	}
	
	
	public void Stat(DT_Sample_4G sample)
	{
		if (sample.LteScRSRP == -1000000) return;
		
		iMRCnt++;
		if (sample.tlte == null || sample.tlte.length == 0)
		{
			iMRCnt_NoNei++;
		}
		else 
		{
			iMRCnt_Nei++;
			
			int rsrp = -1000000;
			for(NC_LTE item:sample.tlte)
			{
				rsrp = Math.max(rsrp, item.LteNcRSRP);
			}
			if(rsrp >= -95)
			{
				iMRCnt_Nei_95++;
			}
			if(rsrp >= -100)
			{
				iMRCnt_Nei_100++;
			}
			if(rsrp >= -103)
			{
				iMRCnt_Nei_103++;
			}
			if(rsrp >= -105)
			{
				iMRCnt_Nei_105++;
			}
			if(rsrp >= -110)
			{
				iMRCnt_Nei_110++;
			}
			if(rsrp >= -113)
			{
				iMRCnt_Nei_113++;
			}
		}
	}
	

	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		String tabMark = ResultHelper.TabMark;
		
		sb.append(iCityID);
		sb.append(tabMark);sb.append(iECI);
		sb.append(tabMark);sb.append(iTime);
		sb.append(tabMark);sb.append(iMRCnt);
		sb.append(tabMark);sb.append(iMRCnt_NoNei);
		sb.append(tabMark);sb.append(iMRCnt_Nei);
		sb.append(tabMark);sb.append(iMRCnt_Nei_95);
		sb.append(tabMark);sb.append(iMRCnt_Nei_100);
		sb.append(tabMark);sb.append(iMRCnt_Nei_103);
		sb.append(tabMark);sb.append(iMRCnt_Nei_105);
		sb.append(tabMark);sb.append(iMRCnt_Nei_110);
		sb.append(tabMark);sb.append(iMRCnt_Nei_113);


		return sb.toString();
	}
	
}
