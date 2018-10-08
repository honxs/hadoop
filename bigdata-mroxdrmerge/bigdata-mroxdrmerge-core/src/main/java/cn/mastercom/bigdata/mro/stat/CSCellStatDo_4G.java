package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.mro.stat.struct.CSStat_Cell;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;
 

public class CSCellStatDo_4G extends AMapStatDo_4G<CSStat_Cell>
{

	public CSCellStatDo_4G(ResultOutputer resultOutputer,int sourceType, int dataType) {
		super(resultOutputer, sourceType, dataType);
		 
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		 
		return new Object[] {sample.Eci,sample.cityID,TimeHelper.getRoundDayTime(sample.itime)};
	}

	@Override
	protected CSStat_Cell createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		 
		CSStat_Cell CS_Cell = new CSStat_Cell();
		CS_Cell.doFirstSample(sample);
		return CS_Cell;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
	 
		return sample.Eci > 0 &&sample.itime > 0&& sample.Eci < Integer.MAX_VALUE&&(sample.flag.equals("EVT")||sample.flag.equals("MRO"));
	}

}
