package cn.mastercom.bigdata.mro.stat;

import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.conf.config.ImeiConfig;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.mro.stat.struct.CSStat_FreqDXLTCell;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSFreqCellLTStatdo_4G  extends AMapStatDo_4G<CSStat_FreqDXLTCell>{
	int i;
	public CSFreqCellLTStatdo_4G(int i,ResultOutputer resultOutputer, int sourceType, int dataType) {
		super(resultOutputer, sourceType, dataType);
		this.i=i;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		 if(i<0)
			 return new Object[] {sample.Eci + "_" + 0,TimeHelper.getRoundDayTime(sample.itime),i};
		 else
		 {
			 return new Object[] {sample.Eci + "_" + sample.getNclte_Freq().get(i).LteNcEarfcn,TimeHelper.getRoundDayTime(sample.itime),i};
		 }
		 
	}

	@Override
	protected CSStat_FreqDXLTCell createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		CSStat_FreqDXLTCell CS_FreqDXLTCell=new CSStat_FreqDXLTCell(); 
		if(i<0)
		{
			CS_FreqDXLTCell.doFirstSample(sample);
		}
		else 
		{
			List<NC_LTE> itemList = sample.getNclte_Freq();
			CS_FreqDXLTCell.doFirstSample(sample, itemList.get(i),i);
		}
		return CS_FreqDXLTCell;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		boolean istrue;
		List<NC_LTE> itemList = sample.getNclte_Freq();
		if(i<0)
		{
			istrue=sample.Eci>0&&sample.Eci < Integer.MAX_VALUE&&sample.flag.equals("MRO")
				&&(ImeiConfig.GetInstance().getValue(sample)==2||ImeiConfig.GetInstance().getValue(sample)==3)
				&&itemList.size()<=0&&sample.itime>0;
		}
		else
		{	
			istrue=sample.Eci>0&&sample.Eci < Integer.MAX_VALUE&&sample.flag.equals("MRO")&&
				i<itemList.size()&&ifLtFcn(itemList.get(i).LteNcEarfcn);
		}
		return istrue;
	}
	public boolean ifLtFcn(int fcn)
	{
		int[] lt_fcn_list = { 1600, 1650, 40340 };
		for (int temfcn : lt_fcn_list)
		{
			if (temfcn == fcn)
			{
				return true;
			}
		}
		return false;
	}
}
