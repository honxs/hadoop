package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Stat_UserInGrid;
import cn.mastercom.bigdata.util.ResultOutputer;


public class UserInGridStatDo_4G extends AMapStatDo_4G<Stat_UserInGrid>{

	public UserInGridStatDo_4G(ResultOutputer resultOutputer, int sourceType, int dataType) {
		super(resultOutputer, sourceType, dataType);
	}
	
	public UserInGridStatDo_4G(ResultOutputer resultOutputer, int sourceType , int confidenceType, int dataType) {
		super(resultOutputer, sourceType, confidenceType,dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		return new Object[]{sample.IMSI,sample.Eci , sample.grid.tllongitude , sample.grid.tllatitude ,sample.grid.brlongitude ,sample.grid.brlatitude};
	}

	@Override
	protected Stat_UserInGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		Stat_UserInGrid stat_UserInGrid = new Stat_UserInGrid();
		stat_UserInGrid.doFirstSample(sample);
		return stat_UserInGrid;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		return sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null && sample.IMSI > 0 && sample.samState == StaticConfig.ACTTYPE_IN;
	}
	
}
