package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;

public class MrInGrid implements IResultTable
{
	public int iCityID = 0;
	public int iBuildingID = 0;
	public int iHeight = 0;
	public int iLongitude = 0;
	public int iLatitude = 0;
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
	public double fRSRPMax = -1000000;
	public double fRSRPMin = -1000000;
	public double fRSRQMax = -1000000;
	public double fRSRQMin = -1000000;
	public double fSINRMax = -1000000;
	public double fSINRMin = -1000000;
	
	
	public MrInGrid(DT_Sample_4G sample, int statTime)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		iLongitude = sample.ilongitude / 1000 * 1000;
		iLatitude = sample.ilatitude / 900 * 900 + 900;
		iTime = statTime;
	}
	
	public static final String TypeName = "mringrid";
	
	public static String getKey(DT_Sample_4G sample)
	{
		int iLotGrid = sample.ilongitude / 1000 * 1000;
		int iLatGrid = sample.ilatitude / 900 * 900 + 900;
		
		//此处按天计算,所以没有添加时间因素
		return sample.cityID 
				+ "_" + sample.ibuildingID 
				+ "_" + sample.iheight 
				+ "_" + iLotGrid 
				+ "_" + iLatGrid;
	}
	
	public void Stat(DT_Sample_4G sample)
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
		
		//fRSRPMax = fRSRPMax == -1000000? sample.LteScRSRP:Math.max(fRSRPMax, sample.LteScRSRP);
		
		fRSRPMax = getMax(fRSRPMax, sample.LteScRSRP); 
		fRSRPMin = getMin(fRSRPMin, sample.LteScRSRP); 
		fRSRQMax = getMax(fRSRQMax, sample.LteScRSRQ);
		fRSRQMin = getMin(fRSRQMin, sample.LteScRSRQ);
		fSINRMax = getMax(fSINRMax, sample.LteScSinrUL);
		fSINRMin = getMin(fSINRMin, sample.LteScSinrUL);
	}
	
	private double getMax( double valueMax, int value)
	{
	    if (valueMax == -1000000 || valueMax < value)
	    {
	    	return value;
	    }
	    
	    return valueMax;
	}
	
	private double getMin( double valueMin, int value)
	{
		if (value == -1000000) return valueMin;
		
	    if (valueMin == -1000000 || valueMin > value)
	    {
	    	return value;
	    }
	    
	    return valueMin;
	}
    
	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		String tabMark = ResultHelper.TabMark;
		
		sb.append(iCityID);
		sb.append(tabMark);sb.append(iBuildingID);
		sb.append(tabMark);sb.append(iHeight);
		sb.append(tabMark);sb.append(iLongitude);
		sb.append(tabMark);sb.append(iLatitude);
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
		sb.append(tabMark);sb.append(fRSRPMax);
		sb.append(tabMark);sb.append(fRSRPMin);
		sb.append(tabMark);sb.append(fRSRQMax);
		sb.append(tabMark);sb.append(fRSRQMin);
		sb.append(tabMark);sb.append(fSINRMax);
		sb.append(tabMark);sb.append(fSINRMin);


		return sb.toString();
	}

}
