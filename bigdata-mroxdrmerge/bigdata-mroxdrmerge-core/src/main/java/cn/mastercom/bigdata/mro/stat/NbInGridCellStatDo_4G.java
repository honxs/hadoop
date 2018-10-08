package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_In_Nb_CellGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;
/**
 * 
 * @author xmr
 *
 */
public class NbInGridCellStatDo_4G extends AMapStatDo_4G<Stat_In_Nb_CellGrid>
{
	/**
	 * 统计的邻区序号
	 */
	private int i;
	
	public NbInGridCellStatDo_4G(int i, ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
		this.i = i;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID, sample.ibuildingID, sample.iheight, sample.tlte[i].LteNcEci, sample.tlte[i].LteNcEarfcn, sample.tlte[i].LteNcPci, sample.grid.tllongitude, sample.grid.tllatitude, FormatTime.RoundTimeForHour(sample.itime), sample.Eci};
	}

	@Override
	protected Stat_In_Nb_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_In_Nb_CellGrid nbInCellGrid = new Stat_In_Nb_CellGrid();
		nbInCellGrid.doFirstSample(sample);
		return nbInCellGrid;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		// 判断第一邻区eci>0,该采样点才进行统计,否则第一邻区为空,后续邻区都是空
		
		return sample.tlte[i] != null && sample.tlte[i].LteNcEci > 0 && sample.ibuildingID > 0 && sample.ilongitude > 0 
				&& sample.ilatitude > 0 && sample.Eci > 0;
	}

}
