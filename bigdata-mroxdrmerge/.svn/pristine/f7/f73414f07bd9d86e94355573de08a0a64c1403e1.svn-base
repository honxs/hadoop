package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Out_Nb_CellGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

/**
 * 
 * @author xmr
 *
 */
public class NbOutGridCellStatDo_4G extends AMapStatDo_4G<Stat_Out_Nb_CellGrid> {
	/**
	 * 统计的邻区序号
	 */
	private int i;
	public NbOutGridCellStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType) {
		super(typeResult, sourceType, dataType);
		
	}

	public NbOutGridCellStatDo_4G(int i, ResultOutputer typeResult, int sourceType, int confidenceType, int dataType) {
		super(typeResult, sourceType, confidenceType, dataType);
		this.i = i;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		return new Object[] { sample.cityID, sample.tlte[i].LteNcEci, sample.tlte[i].LteNcEarfcn,
				sample.tlte[i].LteNcPci, sample.grid.tllongitude, sample.grid.tllatitude,
				FormatTime.RoundTimeForHour(sample.itime), sample.Eci };
	}

	@Override
	protected Stat_Out_Nb_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		Stat_Out_Nb_CellGrid nbOutCellGridStat = new Stat_Out_Nb_CellGrid();
		nbOutCellGridStat.doFirstSample(sample);
		return nbOutCellGridStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		return sample.tlte != null && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.tlte[i].LteNcEci > 0 && sample.Eci > 0;
	}

}
