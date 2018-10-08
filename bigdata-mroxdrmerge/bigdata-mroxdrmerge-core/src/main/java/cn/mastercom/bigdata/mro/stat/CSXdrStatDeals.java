package cn.mastercom.bigdata.mro.stat;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.CompositeStatDo;
import cn.mastercom.bigdata.mro.stat.DayDataDeal_4G;
import cn.mastercom.bigdata.mro.stat.IStatDo;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.util.ResultOutputer;

/**
 * Created by tanpeng on 2018/8/14.
 */
public class CSXdrStatDeals {
	private List<DayDataDeal_4G> dealList;
	public CSXdrStatDeals(ResultOutputer typeResult)
	{
		ArrayList<IStatDo> CSList = new ArrayList<IStatDo>();
		dealList = new ArrayList<DayDataDeal_4G>();
		
		//CS
		CSList.add(new CSCellStatDo_4G(typeResult,StaticConfig.SOURCE_YD,XdrLocTablesEnum.xdrcell.getIndex()));
		CSList.add(new CSCellGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_All,XdrLocTablesEnum.xdrcellgrid.getIndex()));
		CSList.add(new CSGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_All ,XdrLocTablesEnum.xdrgrid.getIndex()));
		CSList.add(new CSGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,XdrLocTablesEnum.xdrgridcqt.getIndex()));
		CSList.add(new CSGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_DT ,XdrLocTablesEnum.xdrgriddt.getIndex()));
	
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(CSList)));
	}
	public void dealSample(DT_Sample_4G sample) {
		for (DayDataDeal_4G deal : dealList) {
			deal.dealSample(sample);
		}
	}
	public int outResult() {
		for (DayDataDeal_4G deal : dealList) {
			deal.outResult();
		}
		return 0;
	}
}
