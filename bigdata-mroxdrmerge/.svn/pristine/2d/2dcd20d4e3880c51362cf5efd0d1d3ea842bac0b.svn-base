package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class MrStatCell implements IResultTable
{
	public int iCityID = 0;
	public int iECI = 0;
	public int iTime = 0;
	public int iMRCnt = 0;
	public int iMRCnt_Indoor = 0;
	public int iMRCnt_Outdoor = 0;
	public int iMRRSRQCnt = 0;
	public int iMRRSRQCnt_Indoor = 0;
	public int iMRRSRQCnt_Outdoor = 0;
	public int iMRSINRCnt = 0;
	public int iMRSINRCnt_Indoor = 0;
	public int iMRSINRCnt_Outdoor = 0;
	public int iMRCnt_Out_URI = 0;
	public int iMRCnt_Out_SDK = 0;
	public int iMRCnt_Out_HIGH = 0;
	public int iMRCnt_Out_SIMU = 0;
	public int iMRCnt_Out_Other = 0;
	public int iMRCnt_In_URI = 0;
	public int iMRCnt_In_SDK = 0;
	public int iMRCnt_In_WLAN = 0;
	public int iMRCnt_In_SIMU = 0;
	public int iMRCnt_In_Other = 0;
	public double fRSRPValue = 0;
	public double fRSRPValue_Indoor = 0;
	public double fRSRPValue_Outdoor = 0;
	public double fRSRQValue = 0;
	public double fRSRQValue_Indoor = 0;
	public double fRSRQValue_Outdoor = 0;
	public double fSINRValue = 0;
	public double fSINRValue_Indoor = 0;
	public double fSINRValue_Outdoor = 0;
	public int iMRCnt_Indoor_0_70 = 0;
	public int iMRCnt_Indoor_70_80 = 0;
	public int iMRCnt_Indoor_80_90 = 0;
	public int iMRCnt_Indoor_90_95 = 0;
	public int iMRCnt_Indoor_100 = 0;
	public int iMRCnt_Indoor_103 = 0;
	public int iMRCnt_Indoor_105 = 0;
	public int iMRCnt_Indoor_110 = 0;
	public int iMRCnt_Indoor_113 = 0;
	public int iMRCnt_Outdoor_0_70 = 0;
	public int iMRCnt_Outdoor_70_80 = 0;
	public int iMRCnt_Outdoor_80_90 = 0;
	public int iMRCnt_Outdoor_90_95 = 0;
	public int iMRCnt_Outdoor_100 = 0;
	public int iMRCnt_Outdoor_103 = 0;
	public int iMRCnt_Outdoor_105 = 0;
	public int iMRCnt_Outdoor_110 = 0;
	public int iMRCnt_Outdoor_113 = 0;
	public int iIndoorRSRP100_SINR0 = 0;
	public int iIndoorRSRP105_SINR0 = 0;
	public int iIndoorRSRP110_SINR3 = 0;
	public int iIndoorRSRP110_SINR0 = 0;
	public int iOutdoorRSRP100_SINR0 = 0;
	public int iOutdoorRSRP105_SINR0 = 0;
	public int iOutdoorRSRP110_SINR3 = 0;
	public int iOutdoorRSRP110_SINR0 = 0;
	public int iSINR_Indoor_0 = 0;
	public int iRSRQ_Indoor_14 = 0;
	public int iSINR_Outdoor_0 = 0;
	public int iRSRQ_Outdoor_14 = 0;
	
	public MrStatCell(DT_Sample_4G sample, int statTime)
	{
		iCityID = sample.cityID;
		iECI = (int)sample.Eci;
		iTime = statTime;
	}
	
	public static final String TypeName = "mrcell";
	
	public static String getKey(DT_Sample_4G sample)
	{
		//此处按天计算,所以没有添加时间因素
		return sample.cityID 
				+ "_" + sample.Eci;
	}
	
	public void Stat(DT_Sample_4G sample)
	{
		if (sample.LteScRSRP != -1000000)
		{
			iMRCnt++;
			
			fRSRPValue += sample.LteScRSRP;
			
			if (sample.testType == StaticConfig.TestType_CQT)
			{
				//iMRCnt_In_URI
				//iMRCnt_In_SDK
				//iMRCnt_In_WLAN
				iMRCnt_In_SIMU++;
				//iMRCnt_In_Other
				
				iMRCnt_Indoor++;
				
				fRSRPValue_Indoor += sample.LteScRSRP;
				if (sample.LteScRSRP >= -70 && sample.LteScRSRP < 0)
				{
					iMRCnt_Indoor_0_70++;
				}
				else if (sample.LteScRSRP >= -80 && sample.LteScRSRP < -70)
				{
					iMRCnt_Indoor_70_80++;
				}
				else if (sample.LteScRSRP >= -90 && sample.LteScRSRP < -80)
				{
					iMRCnt_Indoor_80_90++;
				}
				else if (sample.LteScRSRP >= -95 && sample.LteScRSRP < -90)
				{
					iMRCnt_Indoor_90_95++;
				}

				if (sample.LteScRSRP >= -100 && sample.LteScRSRP < 0)
				{
					iMRCnt_Indoor_100++;
					if (sample.LteScSinrUL >= 0)
					{
						iIndoorRSRP100_SINR0++;
					}
				}
				if (sample.LteScRSRP >= -103 && sample.LteScRSRP < 0)
				{
					iMRCnt_Indoor_103++;
				}
				if (sample.LteScRSRP >= -105 && sample.LteScRSRP < 0)
				{
					iMRCnt_Indoor_105++;
					if (sample.LteScSinrUL >= 0)
					{
						iIndoorRSRP105_SINR0++;
					}
				}
				if (sample.LteScRSRP >= -110 && sample.LteScRSRP < 0)
				{
					iMRCnt_Indoor_110++;
					
					if (sample.LteScSinrUL >= 3)
					{
						iIndoorRSRP110_SINR3++;
					}
					
					if (sample.LteScSinrUL >= 0)
					{
						iIndoorRSRP110_SINR0++;
					}
				}
				if (sample.LteScRSRP >= -113 && sample.LteScRSRP < 0)
				{
					iMRCnt_Indoor_113++;
				}
			}
			else if (sample.testType == StaticConfig.TestType_DT)
			{
				//iMRCnt_Out_URI
				//iMRCnt_Out_SDK
				//iMRCnt_Out_HIGH
				iMRCnt_Out_SIMU++;
				//iMRCnt_Out_Other
				
				iMRCnt_Outdoor++;
				
				fRSRPValue_Outdoor += sample.LteScRSRP;
				
				if (sample.LteScRSRP >= -70 && sample.LteScRSRP < 0)
				{
					iMRCnt_Outdoor_0_70++;
				}
				else if (sample.LteScRSRP >= -80 && sample.LteScRSRP < -70)
				{
					iMRCnt_Outdoor_70_80++;
				}
				else if (sample.LteScRSRP >= -90 && sample.LteScRSRP < -80)
				{
					iMRCnt_Outdoor_80_90++;
				}
				else if (sample.LteScRSRP >= -95 && sample.LteScRSRP < -90)
				{
					iMRCnt_Outdoor_90_95++;
				}
				
				if (sample.LteScRSRP >= -100 && sample.LteScRSRP < 0)
				{
					iMRCnt_Outdoor_100++;
					if (sample.LteScSinrUL >= 0)
					{
						iOutdoorRSRP100_SINR0++;
					}
				}
				if (sample.LteScRSRP >= -103 && sample.LteScRSRP < 0)
				{
					iMRCnt_Outdoor_103++;
				}
				if (sample.LteScRSRP >= -105 && sample.LteScRSRP < 0)
				{
					iMRCnt_Outdoor_105++;
					if (sample.LteScSinrUL >= 0)
					{
						iOutdoorRSRP105_SINR0++;
					}
				}
				if (sample.LteScRSRP >= -110 && sample.LteScRSRP < 0)
				{
					iMRCnt_Outdoor_110++;
					if (sample.LteScSinrUL >= 3)
					{
						iOutdoorRSRP110_SINR3++;
					}
					if (sample.LteScSinrUL >= 0)
					{
						iOutdoorRSRP110_SINR0++;
					}
				}
				if (sample.LteScRSRP >= -113 && sample.LteScRSRP < 0)
				{
					iMRCnt_Outdoor_113++;
				}
			}

		}
		
		if (sample.LteScRSRQ != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += sample.LteScRSRQ;

			if (sample.testType == StaticConfig.TestType_CQT)
			{
				iMRRSRQCnt_Indoor++;
				fRSRQValue_Indoor += sample.LteScRSRQ;
				
				if (sample.LteScRSRQ > -14)
				{
					iRSRQ_Indoor_14++;
				}
			}
			else if (sample.testType == StaticConfig.TestType_DT)
			{
				iMRRSRQCnt_Outdoor++;
				fRSRQValue_Outdoor += sample.LteScRSRQ;
				
				if (sample.LteScRSRQ > -14)
				{
					iRSRQ_Outdoor_14++;
				}
			}
		}
		
		if (sample.LteScSinrUL != -1000000)
		{
			iMRSINRCnt++;
			fSINRValue += sample.LteScSinrUL;
			
			if (sample.testType == StaticConfig.TestType_CQT)
			{
				iMRSINRCnt_Indoor++;
				fSINRValue_Indoor += sample.LteScSinrUL;
				
				if (sample.LteScSinrUL >=0)
				{
					iSINR_Indoor_0++;
				}
				
			}
			else if (sample.testType == StaticConfig.TestType_DT)
			{
				iMRSINRCnt_Outdoor++;
				fSINRValue_Outdoor += sample.LteScSinrUL;
				
				if (sample.LteScSinrUL >=0)
				{
					iSINR_Outdoor_0++;
				}
				
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
		sb.append(tabMark);sb.append(iMRCnt_Indoor);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor);
		sb.append(tabMark);sb.append(iMRRSRQCnt);
		sb.append(tabMark);sb.append(iMRRSRQCnt_Indoor);
		sb.append(tabMark);sb.append(iMRRSRQCnt_Outdoor);
		sb.append(tabMark);sb.append(iMRSINRCnt);
		sb.append(tabMark);sb.append(iMRSINRCnt_Indoor);
		sb.append(tabMark);sb.append(iMRSINRCnt_Outdoor);
		sb.append(tabMark);sb.append(iMRCnt_Out_URI);
		sb.append(tabMark);sb.append(iMRCnt_Out_SDK);
		sb.append(tabMark);sb.append(iMRCnt_Out_HIGH);
		sb.append(tabMark);sb.append(iMRCnt_Out_SIMU);
		sb.append(tabMark);sb.append(iMRCnt_Out_Other);
		sb.append(tabMark);sb.append(iMRCnt_In_URI);
		sb.append(tabMark);sb.append(iMRCnt_In_SDK);
		sb.append(tabMark);sb.append(iMRCnt_In_WLAN);
		sb.append(tabMark);sb.append(iMRCnt_In_SIMU);
		sb.append(tabMark);sb.append(iMRCnt_In_Other);
		sb.append(tabMark);sb.append(fRSRPValue);
		sb.append(tabMark);sb.append(fRSRPValue_Indoor);
		sb.append(tabMark);sb.append(fRSRPValue_Outdoor);
		sb.append(tabMark);sb.append(fRSRQValue);
		sb.append(tabMark);sb.append(fRSRQValue_Indoor);
		sb.append(tabMark);sb.append(fRSRQValue_Outdoor);
		sb.append(tabMark);sb.append(fSINRValue);
		sb.append(tabMark);sb.append(fSINRValue_Indoor);
		sb.append(tabMark);sb.append(fSINRValue_Outdoor);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_0_70);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_70_80);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_80_90);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_90_95);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_100);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_103);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_105);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_110);
		sb.append(tabMark);sb.append(iMRCnt_Indoor_113);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_0_70);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_70_80);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_80_90);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_90_95);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_100);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_103);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_105);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_110);
		sb.append(tabMark);sb.append(iMRCnt_Outdoor_113);
		sb.append(tabMark);sb.append(iIndoorRSRP100_SINR0);
		sb.append(tabMark);sb.append(iIndoorRSRP105_SINR0);
		sb.append(tabMark);sb.append(iIndoorRSRP110_SINR3);
		sb.append(tabMark);sb.append(iIndoorRSRP110_SINR0);
		sb.append(tabMark);sb.append(iOutdoorRSRP100_SINR0);
		sb.append(tabMark);sb.append(iOutdoorRSRP105_SINR0);
		sb.append(tabMark);sb.append(iOutdoorRSRP110_SINR3);
		sb.append(tabMark);sb.append(iOutdoorRSRP110_SINR0);
		sb.append(tabMark);sb.append(iSINR_Indoor_0);
		sb.append(tabMark);sb.append(iRSRQ_Indoor_14);
		sb.append(tabMark);sb.append(iSINR_Outdoor_0);
		sb.append(tabMark);sb.append(iRSRQ_Outdoor_14);

		return sb.toString();
	}

}
