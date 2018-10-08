package cn.mastercom.bigdata.xdr.loc;

import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;

public class MrBuildCell implements IResultTable
{
	public int iCityID = 0;
	public int iBuildingID = 0;
	public int iECI = 0;
	public int iTime = 0;
	public int iMRCnt = 0;
	public int iMRCnt_In_URI = 0;
	public int iMRCnt_In_SDK = 0;
	public int iMRCnt_In_WLAN = 0;
	public int iMRCnt_In_SIMU = 0;
	public int iMRCnt_In_Other = 0;
	public int iMRRSRQCnt = 0;
	public int iMRSINRCnt = 0;
	public double fRSRPValue = 0;
	public double fRSRQValue = 0;
	public double fSINRValue = 0;
	public int iMRCnt_95 = 0;
	public int iMRCnt_100 = 0;
	public int iMRCnt_103 = 0;
	public int iMRCnt_105 = 0;
	public int iMRCnt_110 = 0;
	public int iMRCnt_113 = 0;
	public int iRSRP100_SINR0 = 0;
	public int iRSRP105_SINR0 = 0;
	public int iRSRP110_SINR3 = 0;
	public int iRSRP110_SINR0 = 0;
	public int iSINR_0 = 0;
	public int iRSRQ_14 = 0;
	public int iASNei_MRCnt = 0;
	public double fASNei_RSRPValue = 0;

	
	public MrBuildCell(DT_Sample_4G sample, int statTime)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iECI = (int)sample.Eci;
		iTime = statTime;
	}
	
	public static final String TypeName = "mrbuildcell";
	
	public static String getKey(DT_Sample_4G sample)
	{
		//此处按天计算,所以没有添加时间因素
		return sample.cityID 
				+ "_" + sample.ibuildingID
				+ "_" + sample.Eci;
	}
	
	public void Stat(DT_Sample_4G sample, Map<String, IResultTable> map)
	{
		if (sample.LteScRSRP == -1000000) return;
		
		iMRCnt++;
		iMRCnt_In_SIMU++;
		
		fRSRPValue += sample.LteScRSRP;
		
		if (sample.LteScRSRQ != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += sample.LteScRSRQ;
		}
		
		if (sample.LteScSinrUL != -1000000)
		{
			iMRSINRCnt++;
			fSINRValue += sample.LteScSinrUL;
		}
		
		if (sample.LteScRSRP >= -95)
		{
			iMRCnt_95++;
		}
		
		if (sample.LteScRSRP >= -100)
		{
			iMRCnt_100++;
			if (sample.LteScSinrUL >= 0)
			{
				iRSRP100_SINR0++;
			}
		}
		
		if (sample.LteScRSRP >= -103)
		{
			iMRCnt_103++;
		}
		
		if (sample.LteScRSRP >= -105)
		{
			iMRCnt_105++;
			if (sample.LteScSinrUL >= 0)
			{
				iRSRP105_SINR0++;
			}
		}
		
		if (sample.LteScRSRP >= -110)
		{
			iMRCnt_110++;
			if (sample.LteScSinrUL >= 3)
			{
				iRSRP110_SINR3++;
			}
			if (sample.LteScSinrUL >= 0)
			{
				iRSRP110_SINR0++;
			}
		}
		
		if (sample.LteScRSRP >= -113)
		{
			iMRCnt_113++;
		}
		
		if (sample.LteScSinrUL >= 0)
		{
			iSINR_0++;
		}

		if (sample.LteScRSRQ > -14)
		{
			iRSRQ_14++;
		}
		
		if (sample.tlte != null)
		{
			for(NC_LTE nclte:sample.tlte)
			{
				statNc(map, nclte);
			}
		}
	}
	
	private void statNc(Map<String, IResultTable> map, NC_LTE nclte)
	{
		String key = MrBuildCellNc.getKey(this, nclte);
		MrBuildCellNc item = (MrBuildCellNc) map.get(key);
		if (item == null)
		{
			item = new MrBuildCellNc(this, nclte);
			map.put(key, item);
		}
		
		item.Stat(nclte);
	}
	

	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		String tabMark = ResultHelper.TabMark;
		
		sb.append(iCityID);
		sb.append(tabMark);sb.append(iBuildingID);
		sb.append(tabMark);sb.append(iECI);
		sb.append(tabMark);sb.append(iTime);
		sb.append(tabMark);sb.append(iMRCnt);
		sb.append(tabMark);sb.append(iMRCnt_In_URI);
		sb.append(tabMark);sb.append(iMRCnt_In_SDK);
		sb.append(tabMark);sb.append(iMRCnt_In_WLAN);
		sb.append(tabMark);sb.append(iMRCnt_In_SIMU);
		sb.append(tabMark);sb.append(iMRCnt_In_Other);
		sb.append(tabMark);sb.append(iMRRSRQCnt);
		sb.append(tabMark);sb.append(iMRSINRCnt);
		sb.append(tabMark);sb.append(fRSRPValue);
		sb.append(tabMark);sb.append(fRSRQValue);
		sb.append(tabMark);sb.append(fSINRValue);
		sb.append(tabMark);sb.append(iMRCnt_95);
		sb.append(tabMark);sb.append(iMRCnt_100);
		sb.append(tabMark);sb.append(iMRCnt_103);
		sb.append(tabMark);sb.append(iMRCnt_105);
		sb.append(tabMark);sb.append(iMRCnt_110);
		sb.append(tabMark);sb.append(iMRCnt_113);
		sb.append(tabMark);sb.append(iRSRP100_SINR0);
		sb.append(tabMark);sb.append(iRSRP105_SINR0);
		sb.append(tabMark);sb.append(iRSRP110_SINR3);
		sb.append(tabMark);sb.append(iRSRP110_SINR0);
		sb.append(tabMark);sb.append(iSINR_0);
		sb.append(tabMark);sb.append(iRSRQ_14);
		sb.append(tabMark);sb.append(iASNei_MRCnt);
		sb.append(tabMark);sb.append(fASNei_RSRPValue);


		return sb.toString();
	}
	
}
