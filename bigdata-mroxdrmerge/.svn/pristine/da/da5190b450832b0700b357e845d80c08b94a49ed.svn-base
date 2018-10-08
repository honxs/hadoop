package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Stat_UserOut_CellGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class UserOutCellGridStatDo_4G extends AMapStatDo_4G<Stat_UserOut_CellGrid>{

	public UserOutCellGridStatDo_4G(ResultOutputer resultOutputer, int sourceType, int dataType) {
		super(resultOutputer, sourceType, dataType);
	}
	
	public UserOutCellGridStatDo_4G(ResultOutputer resultOutputer, int sourceType , int confidenceType, int dataType) {
		super(resultOutputer, sourceType ,confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		return new Object[]{sample.IMSI ,sample.Eci, sample.grid.tllongitude , sample.grid.tllatitude ,FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_UserOut_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		Stat_UserOut_CellGrid stat_UserOutGrid = new Stat_UserOut_CellGrid();
		stat_UserOutGrid.doFirstSample(sample);
		return stat_UserOutGrid;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		return sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null && sample.IMSI > 0 && sample.samState == StaticConfig.ACTTYPE_OUT;
	}
	
}
