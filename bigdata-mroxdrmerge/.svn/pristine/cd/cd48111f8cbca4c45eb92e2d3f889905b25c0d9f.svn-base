package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Stat_UserIn_CellGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class UserInCellGridStatDo_4G extends AMapStatDo_4G<Stat_UserIn_CellGrid>{

	public UserInCellGridStatDo_4G(ResultOutputer resultOutputer, int sourceType, int dataType) {
		super(resultOutputer, sourceType, dataType);
	}
	
	public UserInCellGridStatDo_4G(ResultOutputer resultOutputer, int sourceType , int confidenceType, int dataType) {
		super(resultOutputer, sourceType, confidenceType,dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		return new Object[]{sample.IMSI, sample.ibuildingID, sample.iheight, sample.Eci, sample.grid.tllongitude, sample.grid.tllatitude, FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_UserIn_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		Stat_UserIn_CellGrid stat_UserInGrid = new Stat_UserIn_CellGrid();
		stat_UserInGrid.doFirstSample(sample);
		return stat_UserInGrid;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		return sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.IMSI > 0 && sample.ibuildingID > 0;
	}
	
}
