package cn.mastercom.bigdata.mro.stat;

import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.conf.config.ImeiConfig;
import cn.mastercom.bigdata.mro.stat.struct.CSStat_TenDXLTFreqGrid;
import cn.mastercom.bigdata.util.ResultOutputer;

public class CSTenFreqGridDXStatdo_4G extends AMapStatDo_4G<CSStat_TenDXLTFreqGrid>{
	int i,sourceType;
	public CSTenFreqGridDXStatdo_4G(int i,ResultOutputer resultOutputer, int sourceType1,int sourceType, int dataType)  {
		super(resultOutputer, sourceType1, dataType);
		this.i=i;
		this.sourceType=sourceType;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		 if(i<0)
		 {
			 return new Object[] {sample.cityID + "_" + (sample.ilongitude / 1000) * 1000 + "_" + (sample.ilatitude / 900) * 900 + 900 + "_" +0};
		 }
		 else
		 {
			 List<NC_LTE> itemList = sample.getNclte_Freq();
			 return new Object[] {sample.cityID + "_" + (sample.ilongitude / 1000) * 1000 + "_" + (sample.ilatitude / 900) * 900 + 900 + "_" + itemList.get(i).LteNcEarfcn};
		 }
	}

	@Override
	protected CSStat_TenDXLTFreqGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		CSStat_TenDXLTFreqGrid CSDXLTTenFreqGrid=new CSStat_TenDXLTFreqGrid();
		if(i<0)
			CSDXLTTenFreqGrid.doFirstSample(sample);
		else
		{	
			List<NC_LTE> itemList = sample.getNclte_Freq();
			CSDXLTTenFreqGrid.doFirstSample(sample, itemList.get(i));
		}
		return CSDXLTTenFreqGrid;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		boolean istrue;
		List<NC_LTE> itemList = sample.getNclte_Freq();
		if(i<0)
		{
			istrue= (sample.flag.equals("MRO")||sample.flag.equals("MRE"))
				&&(ImeiConfig.GetInstance().getValue(sample)==2||ImeiConfig.GetInstance().getValue(sample)==3)
				&&itemList.size()<=0;
		}
		else
		{	
			istrue= (sample.flag.equals("MRO")||sample.flag.equals("MRE"))&&
				i<itemList.size()&&ifDxFcn(itemList.get(i).LteNcEarfcn);
		}
		return istrue;
	}
	public boolean ifDxFcn(int fcn)
	{
		int[] dx_fcn_list = { 1775, 1800, 1825, 1850, 1870, 75, 100 };
		for (int temfcn : dx_fcn_list)
		{
			if (temfcn == fcn)
			{
				return true;
			}
		}

		if (fcn >= 2410 && fcn <= 2510)
		{// 800M
			return true;
		}
		return false;
	}
}
 
