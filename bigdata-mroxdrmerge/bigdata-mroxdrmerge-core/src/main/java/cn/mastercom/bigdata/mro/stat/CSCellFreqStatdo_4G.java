package cn.mastercom.bigdata.mro.stat;

import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.mro.stat.struct.CSStat_CellFreq;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSCellFreqStatdo_4G extends AMapStatDo_4G<CSStat_CellFreq>{
	int i;
	public CSCellFreqStatdo_4G(int i,ResultOutputer resultOutputer, int sourceType, int dataType) {
		super(resultOutputer, sourceType, dataType);
		this.i=i;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		List<NC_LTE> itemList = sample.getNclte_Freq();
		return new Object[] {sample.Eci,itemList.get(i).LteNcEarfcn,itemList.get(i).LteNcPci,TimeHelper.getRoundDayTime(sample.itime),i};
	}

	@Override
	protected CSStat_CellFreq createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		CSStat_CellFreq CS_CellFreq=new CSStat_CellFreq();
		List<NC_LTE> itemList = sample.getNclte_Freq();
		CS_CellFreq.doFirstSample(sample, itemList.get(i),i);
		return CS_CellFreq;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		 boolean istrue;
		 List<NC_LTE> itemList = sample.getNclte_Freq();
		 istrue=sample.Eci>0&&sample.Eci < Integer.MAX_VALUE&&i<itemList.size()
				 &&sample.flag.equals("MRO")&&sample.itime>0;
		return istrue ;
	}

}
