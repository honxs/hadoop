package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSStat_TenDXLTFreqGrid implements IStat_4G,Serializable{
	protected int icityid;
	protected int startTime;
	protected int endTime;
	protected int freq;
	protected int itllongitude;
	protected int itllatitude;
	protected int ibrlongitude;
	protected int ibrlatitude;
	protected int iMRCnt;
	protected double fRSRPValue;
	protected int iMRCnt_95;
	protected int iMRCnt_100;
	protected int iMRCnt_103;
	protected int iMRCnt_105;
	protected int iMRCnt_110;
	protected int iMRCnt_113;
	protected int iMRCnt_128;
	protected int iMRRSRQCnt;
	protected double fRSRQValue;
	protected int iNCMRCnt;
	protected double fNCRSRPValue;
	protected int iNCMRCnt_95;
	protected int iNCMRCnt_100;
	protected int iNCMRCnt_103;
	protected int iNCMRCnt_105;
	protected int iNCMRCnt_110;
	protected int iNCMRCnt_113;
	protected int iNCMRCnt_128;
	protected int iNCMRRSRQCnt;
	protected double fNCRSRQValue;
	int ncRsrp;
	int ncRsrq;
	public void doFirstSample(DT_Sample_4G sample ,NC_LTE nc_LTE)
	{
		ncRsrp=nc_LTE.LteNcRSRP;
		ncRsrq=nc_LTE.LteNcRSRQ;
		
		icityid = sample.cityID;
		startTime=TimeHelper.getRoundDayTime(sample.itime);
		endTime=startTime+ 86400;
		freq = nc_LTE.LteNcEarfcn;
		itllongitude = (sample.ilongitude / 1000) * 1000;
		itllatitude = (sample.ilatitude / 900) * 900 + 900;
		ibrlongitude = itllongitude+ 1000;
		ibrlatitude = itllatitude-900;
	}
	public void doFirstSample(DT_Sample_4G sample)
	{
		ncRsrp=StaticConfig.Int_Abnormal;
		ncRsrq=StaticConfig.Int_Abnormal;
		
		icityid = sample.cityID;
		startTime=TimeHelper.getRoundDayTime(sample.itime);
		endTime=startTime+ 86400;
		freq = 0;
		itllongitude = (sample.ilongitude / 1000) * 1000;
		itllatitude = (sample.ilatitude / 900) * 900 + 900;
		ibrlongitude = itllongitude+ 1000;
		ibrlatitude = itllatitude-900;
	}
	@Override
	public void doSample(DT_Sample_4G sample) {

		if (!(sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30))
		{
			return;
		}
		iMRCnt++;
		fRSRPValue += sample.LteScRSRP;
		if (sample.LteScRSRP >= -95)
		{
			iMRCnt_95++;
		}
		if (sample.LteScRSRP >= -100)
		{
			iMRCnt_100++;
		}
		if (sample.LteScRSRP >= -103)
		{
			iMRCnt_103++;
		}
		if (sample.LteScRSRP >= -105)
		{
			iMRCnt_105++;
		}
		if (sample.LteScRSRP >= -110)
		{
			iMRCnt_110++;
		}
		if (sample.LteScRSRP >= -113)
		{
			iMRCnt_113++;
		}
		if (sample.LteScRSRP >= -128)
		{
			iMRCnt_128++;
		}
		if (sample.LteScRSRQ != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += sample.LteScRSRQ;
		}
		if (ncRsrp >= -150 && ncRsrp <= -30)
		{
			iNCMRCnt++;
			fNCRSRPValue += ncRsrp;
		}
		if (ncRsrp >= -95)
		{
			iNCMRCnt_95++;
		}
		if (ncRsrp >= -100)
		{
			iNCMRCnt_100++;
		}
		if (ncRsrp >= -103)
		{
			iNCMRCnt_103++;
		}
		if (ncRsrp >= -105)
		{
			iNCMRCnt_105++;
		}
		if (ncRsrp >= -110)
		{
			iNCMRCnt_110++;
		}
		if (ncRsrp >=-113)
		{
			iNCMRCnt_113++;
		}
		if (ncRsrp >= -128)
		{
			iNCMRCnt_128++;
		}
		if (ncRsrq != -1000000)
		{
			iNCMRRSRQCnt++;
			fNCRSRQValue += ncRsrq;
		}
	
		
	}

	@Override
	public void doSampleLT(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSampleDX(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toLine() {
		StringBuffer bf = new StringBuffer();
		bf.append(icityid);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(startTime);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(endTime);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(freq);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(itllongitude);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(itllatitude);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(ibrlongitude);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(ibrlatitude);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iMRCnt);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(fRSRPValue);
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
		bf.append(iMRRSRQCnt);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(fRSRQValue);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(fNCRSRPValue);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_95);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_100);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_103);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_105);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_110);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_113);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRCnt_128);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(iNCMRRSRQCnt);
		bf.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		bf.append(fNCRSRQValue);
		return bf.toString();
	}

}
