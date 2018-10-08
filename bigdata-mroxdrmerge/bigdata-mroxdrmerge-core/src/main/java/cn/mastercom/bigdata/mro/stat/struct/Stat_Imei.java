package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_Imei implements IStat_4G,Serializable
{
	public int iCityID;
	public int imeiTac;
	public int ifreq;
	public int iTime;
	public int iMRCnt;
	public int iMRCnt_Indoor;
	public int iMRCnt_Outdoor;
	public int iMRRSRQCnt;
	public int iMRRSRQCnt_Indoor;
	public int iMRRSRQCnt_Outdoor;
	public int iMRSINRCnt;
	public int iMRSINRCnt_Indoor;
	public int iMRSINRCnt_Outdoor;
	public float fRSRPValue;
	public float fRSRPValue_Indoor;
	public float fRSRPValue_Outdoor;
	public float fRSRQValue;
	public float fRSRQValue_Indoor;
	public float fRSRQValue_Outdoor;
	public float fSINRValue;
	public float fSINRValue_Indoor;
	public float fSINRValue_Outdoor;
	public int iMRCnt_Indoor_0_70;
	public int iMRCnt_Indoor_70_80;
	public int iMRCnt_Indoor_80_90;
	public int iMRCnt_Indoor_90_95;
	public int iMRCnt_Indoor_100;
	public int iMRCnt_Indoor_103;
	public int iMRCnt_Indoor_105;
	public int iMRCnt_Indoor_110;
	public int iMRCnt_Indoor_113;
	public int iMRCnt_Outdoor_0_70;
	public int iMRCnt_Outdoor_70_80;
	public int iMRCnt_Outdoor_80_90;
	public int iMRCnt_Outdoor_90_95;
	public int iMRCnt_Outdoor_100;
	public int iMRCnt_Outdoor_103;
	public int iMRCnt_Outdoor_105;
	public int iMRCnt_Outdoor_110;
	public int iMRCnt_Outdoor_113;
	public int iIndoorRSRP100_SINR0;
	public int iIndoorRSRP105_SINR0;
	public int iIndoorRSRP110_SINR3;
	public int iIndoorRSRP110_SINR0;
	public int iOutdoorRSRP100_SINR0;
	public int iOutdoorRSRP105_SINR0;
	public int iOutdoorRSRP110_SINR3;
	public int iOutdoorRSRP110_SINR0;
	public int iSINR_Indoor_0;
	public int iRSRQ_Indoor_14;
	public int iSINR_Outdoor_0;
	public int iRSRQ_Outdoor_14;
	public float fOverlapTotal;
	public int iOverlapMRCnt;
	public float fOverlapTotalAll;
	public int iOverlapMRCntAll;
	//
	public int MRCnt_0_70;
	public int MRCnt_70_80;
	public int MRCnt_80_90;
	public int MRCnt_90_95;
	public int MRCnt_100;
	public int MRCnt_103;
	public int MRCnt_105;
	public int MRCnt_110;
	public int MRCnt_113;
	public int RSRP100_SINR0;
	public int RSRP105_SINR0;
	public int RSRP110_SINR3;
	public int RSRP110_SINR0;
	public int SINR_0;
	public int RSRQ_14;
	public static final String spliter = "\t";

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		imeiTac = sample.imeiTac;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
	}

	public void doSample(int rsrp, int rsrq, int sinrul, int samState, int Overlap, int OverlapAll)
	{
		if (rsrp >= -150 && rsrp <= -30)
		{
			iMRCnt++;

			fRSRPValue += rsrp;
			if (rsrp >= -70 && rsrp < 0)
			{
				MRCnt_0_70++;
			}
			else if (rsrp >= -80 && rsrp < -70)
			{
				MRCnt_70_80++;
			}
			else if (rsrp >= -90 && rsrp < -80)
			{
				MRCnt_80_90++;
			}
			else if (rsrp >= -95 && rsrp < -90)
			{
				MRCnt_90_95++;
			}

			if (rsrp >= -100 && rsrp < 0)
			{
				MRCnt_100++;
				if (sinrul >= 0)
				{
					RSRP100_SINR0++;
				}
			}
			if (rsrp >= -103 && rsrp < 0)
			{
				MRCnt_103++;
			}
			if (rsrp >= -105 && rsrp < 0)
			{
				MRCnt_105++;
				if (sinrul >= 0)
				{
					RSRP105_SINR0++;
				}
			}
			if (rsrp >= -110 && rsrp < 0)
			{
				MRCnt_110++;

				if (sinrul >= 3)
				{
					RSRP110_SINR3++;
				}

				if (sinrul >= 0)
				{
					RSRP110_SINR0++;
				}
			}
			if (rsrp >= -113 && rsrp < 0)
			{
				MRCnt_113++;
			}
			if (rsrq > -14)
			{
				RSRQ_14++;
			}
			if (sinrul >= 0)
			{
				SINR_0++;
			}

			if (samState == StaticConfig.ACTTYPE_IN)
			{
				iMRCnt_Indoor++;

				fRSRPValue_Indoor += rsrp;
				if (rsrp >= -70 && rsrp < 0)
				{
					iMRCnt_Indoor_0_70++;
				}
				else if (rsrp >= -80 && rsrp < -70)
				{
					iMRCnt_Indoor_70_80++;
				}
				else if (rsrp >= -90 && rsrp < -80)
				{
					iMRCnt_Indoor_80_90++;
				}
				else if (rsrp >= -95 && rsrp < -90)
				{
					iMRCnt_Indoor_90_95++;
				}

				if (rsrp >= -100 && rsrp < 0)
				{
					iMRCnt_Indoor_100++;
					if (sinrul >= 0)
					{
						iIndoorRSRP100_SINR0++;
					}
				}
				if (rsrp >= -103 && rsrp < 0)
				{
					iMRCnt_Indoor_103++;
				}
				if (rsrp >= -105 && rsrp < 0)
				{
					iMRCnt_Indoor_105++;
					if (sinrul >= 0)
					{
						iIndoorRSRP105_SINR0++;
					}
				}
				if (rsrp >= -110 && rsrp < 0)
				{
					iMRCnt_Indoor_110++;

					if (sinrul >= 3)
					{
						iIndoorRSRP110_SINR3++;
					}

					if (sinrul >= 0)
					{
						iIndoorRSRP110_SINR0++;
					}
				}
				if (rsrp >= -113 && rsrp < 0)
				{
					iMRCnt_Indoor_113++;
				}
			}
			else if (samState == StaticConfig.ACTTYPE_OUT)
			{
				iMRCnt_Outdoor++;

				fRSRPValue_Outdoor += rsrp;

				if (rsrp >= -70 && rsrp < 0)
				{
					iMRCnt_Outdoor_0_70++;
				}
				else if (rsrp >= -80 && rsrp < -70)
				{
					iMRCnt_Outdoor_70_80++;
				}
				else if (rsrp >= -90 && rsrp < -80)
				{
					iMRCnt_Outdoor_80_90++;
				}
				else if (rsrp >= -95 && rsrp < -90)
				{
					iMRCnt_Outdoor_90_95++;
				}

				if (rsrp >= -100 && rsrp < 0)
				{
					iMRCnt_Outdoor_100++;
					if (sinrul >= 0)
					{
						iOutdoorRSRP100_SINR0++;
					}
				}
				if (rsrp >= -103 && rsrp < 0)
				{
					iMRCnt_Outdoor_103++;
				}
				if (rsrp >= -105 && rsrp < 0)
				{
					iMRCnt_Outdoor_105++;
					if (sinrul >= 0)
					{
						iOutdoorRSRP105_SINR0++;
					}
				}
				if (rsrp >= -110 && rsrp < 0)
				{
					iMRCnt_Outdoor_110++;
					if (sinrul >= 3)
					{
						iOutdoorRSRP110_SINR3++;
					}
					if (sinrul >= 0)
					{
						iOutdoorRSRP110_SINR0++;
					}
				}
				if (rsrp >= -113 && rsrp < 0)
				{
					iMRCnt_Outdoor_113++;
				}
			}
		}

		if (rsrq > -10000)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;

			if (samState == StaticConfig.ACTTYPE_IN)
			{
				iMRRSRQCnt_Indoor++;
				fRSRQValue_Indoor += rsrq;

				if (rsrq > -14)
				{
					iRSRQ_Indoor_14++;
				}
			}
			else if (samState == StaticConfig.ACTTYPE_OUT)
			{
				iMRRSRQCnt_Outdoor++;
				fRSRQValue_Outdoor += rsrq;

				if (rsrq > -14)
				{
					iRSRQ_Outdoor_14++;
				}
			}
		}

		if (sinrul >= -1000 && sinrul <= 1000)
		{
			iMRSINRCnt++;
			fSINRValue += sinrul;

			if (samState == StaticConfig.ACTTYPE_IN)
			{
				iMRSINRCnt_Indoor++;
				fSINRValue_Indoor += sinrul;

				if (sinrul >= 0)
				{
					iSINR_Indoor_0++;
				}

			}
			else if (samState == StaticConfig.ACTTYPE_OUT)
			{
				iMRSINRCnt_Outdoor++;
				fSINRValue_Outdoor += sinrul;

				if (sinrul >= 0)
				{
					iSINR_Outdoor_0++;
				}
			}
			fOverlapTotal += Overlap;
			if (Overlap >= 4)
			{
				iOverlapMRCnt++;
			}
			fOverlapTotalAll += OverlapAll;
			if (OverlapAll >= 4)
			{
				iOverlapMRCntAll++;
			}
		}
	}

	public void doSample(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.samState, sample.overlapSameEarfcn,
				sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
	{
		doSample(sample.lt_freq[0].LteNcRSRP, sample.lt_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.samState, sample.overlapSameEarfcn,
				sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.dx_freq[0].LteNcRSRP, sample.dx_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.samState, sample.overlapSameEarfcn,
				sample.OverlapAll);
	}

	@Override
	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(imeiTac);
		bf.append(spliter);
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRRSRQCnt_Outdoor);
		bf.append(spliter);
		bf.append(iMRSINRCnt);
		bf.append(spliter);
		bf.append(iMRSINRCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRSINRCnt_Outdoor);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRPValue_Indoor);
		bf.append(spliter);
		bf.append(fRSRPValue_Outdoor);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(fRSRQValue_Indoor);
		bf.append(spliter);
		bf.append(fRSRQValue_Outdoor);
		bf.append(spliter);
		bf.append(fSINRValue);
		bf.append(spliter);
		bf.append(fSINRValue_Indoor);
		bf.append(spliter);
		bf.append(fSINRValue_Outdoor);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_100);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_103);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_105);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_110);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_113);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_100);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_103);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_105);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_110);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_113);
		bf.append(spliter);
		bf.append(iIndoorRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iIndoorRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iIndoorRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iIndoorRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iOutdoorRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iSINR_Indoor_0);
		bf.append(spliter);
		bf.append(iRSRQ_Indoor_14);
		bf.append(spliter);
		bf.append(iSINR_Outdoor_0);
		bf.append(spliter);
		bf.append(iRSRQ_Outdoor_14);
		bf.append(spliter);
		bf.append(fOverlapTotal);
		bf.append(spliter);
		bf.append(iOverlapMRCnt);
		bf.append(spliter);
		bf.append(fOverlapTotalAll);
		bf.append(spliter);
		bf.append(iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(MRCnt_0_70);
		bf.append(spliter);
		bf.append(MRCnt_70_80);
		bf.append(spliter);
		bf.append(MRCnt_80_90);
		bf.append(spliter);
		bf.append(MRCnt_90_95);
		bf.append(spliter);
		bf.append(MRCnt_100);
		bf.append(spliter);
		bf.append(MRCnt_103);
		bf.append(spliter);
		bf.append(MRCnt_105);
		bf.append(spliter);
		bf.append(MRCnt_110);
		bf.append(spliter);
		bf.append(MRCnt_113);
		bf.append(spliter);
		bf.append(RSRP100_SINR0);
		bf.append(spliter);
		bf.append(RSRP105_SINR0);
		bf.append(spliter);
		bf.append(RSRP110_SINR3);
		bf.append(spliter);
		bf.append(RSRP110_SINR0);
		bf.append(spliter);
		bf.append(SINR_0);
		bf.append(spliter);
		bf.append(RSRQ_14);
		return bf.toString();
	}

	public static Stat_Imei FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_Imei stat_imei = new Stat_Imei();
		try
		{
			stat_imei.iCityID = Integer.parseInt(vals[i++]);
			stat_imei.imeiTac = Integer.parseInt(vals[i++]);
			stat_imei.ifreq = Integer.parseInt(vals[i++]);
			stat_imei.iTime = FormatTime.getRoundDayTime(Integer.parseInt(vals[i++]));
			stat_imei.iMRCnt = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor = Integer.parseInt(vals[i++]);
			stat_imei.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			stat_imei.iMRRSRQCnt_Indoor = Integer.parseInt(vals[i++]);
			stat_imei.iMRRSRQCnt_Outdoor = Integer.parseInt(vals[i++]);
			stat_imei.iMRSINRCnt = Integer.parseInt(vals[i++]);
			stat_imei.iMRSINRCnt_Indoor = Integer.parseInt(vals[i++]);
			stat_imei.iMRSINRCnt_Outdoor = Integer.parseInt(vals[i++]);

			stat_imei.fRSRPValue = Float.parseFloat(vals[i++]);
			stat_imei.fRSRPValue_Indoor = Float.parseFloat(vals[i++]);
			stat_imei.fRSRPValue_Outdoor = Float.parseFloat(vals[i++]);
			stat_imei.fRSRQValue = Float.parseFloat(vals[i++]);
			stat_imei.fRSRQValue_Indoor = Float.parseFloat(vals[i++]);
			stat_imei.fRSRQValue_Outdoor = Float.parseFloat(vals[i++]);
			stat_imei.fSINRValue = Float.parseFloat(vals[i++]);
			stat_imei.fSINRValue_Indoor = Float.parseFloat(vals[i++]);
			stat_imei.fSINRValue_Outdoor = Float.parseFloat(vals[i++]);

			stat_imei.iMRCnt_Indoor_0_70 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_70_80 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_80_90 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_90_95 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_100 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_103 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_105 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_110 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Indoor_113 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_0_70 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_70_80 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_80_90 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_90_95 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_100 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_103 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_105 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_110 = Integer.parseInt(vals[i++]);
			stat_imei.iMRCnt_Outdoor_113 = Integer.parseInt(vals[i++]);
			stat_imei.iIndoorRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.iIndoorRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.iIndoorRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			stat_imei.iIndoorRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.iOutdoorRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.iOutdoorRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.iOutdoorRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			stat_imei.iOutdoorRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.iSINR_Indoor_0 = Integer.parseInt(vals[i++]);
			stat_imei.iRSRQ_Indoor_14 = Integer.parseInt(vals[i++]);
			stat_imei.iSINR_Outdoor_0 = Integer.parseInt(vals[i++]);
			stat_imei.iRSRQ_Outdoor_14 = Integer.parseInt(vals[i++]);
			stat_imei.fOverlapTotal = Float.parseFloat(vals[i++]);
			stat_imei.iOverlapMRCnt = Integer.parseInt(vals[i++]);
			stat_imei.fOverlapTotalAll = Float.parseFloat(vals[i++]);
			stat_imei.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
			
			stat_imei.MRCnt_0_70 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_70_80 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_80_90 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_90_95 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_100 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_103 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_105 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_110 = Integer.parseInt(vals[i++]);
			stat_imei.MRCnt_113 = Integer.parseInt(vals[i++]);
			stat_imei.RSRP100_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.RSRP105_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.RSRP110_SINR3 = Integer.parseInt(vals[i++]);
			stat_imei.RSRP110_SINR0 = Integer.parseInt(vals[i++]);
			stat_imei.SINR_0 = Integer.parseInt(vals[i++]);
			stat_imei.RSRQ_14 = Integer.parseInt(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"Stat_Imei filldata error", "", e);
		}
		return stat_imei;
	}
}
