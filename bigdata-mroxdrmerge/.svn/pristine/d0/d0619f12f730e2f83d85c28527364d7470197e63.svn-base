package cn.mastercom.bigdata.mro.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.mro.stat.CSCellFreqStatdo_4G;
import cn.mastercom.bigdata.mro.stat.struct.IStat_4G;
/**
 * Created by tanpeng on 2018/8/14.
 */
public class CSMroStatDeals {
	private List<DayDataDeal_4G> dealList;
	public CSMroStatDeals(ResultOutputer typeResult)
	{
		ArrayList<IStatDo> CSList = new ArrayList<IStatDo>();
		dealList = new ArrayList<DayDataDeal_4G>();
		CSList.add(new CSCellStatDo_4G(typeResult,StaticConfig.SOURCE_YD,MroCsOTTTableEnum.mrocell.getIndex()));
		CSList.add(new CSCellGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_All,MroCsOTTTableEnum.mrocellgrid.getIndex()));
		
		CSList.add(new CSTenCellGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_All,MroCsOTTTableEnum.tenmrocellgrid.getIndex()));
		CSList.add(new CSTenCellGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.tencellgridcqt.getIndex()));
		CSList.add(new CSTenCellGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.tencellgriddt.getIndex()));
		
		CSList.add(new CSGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_All ,MroCsOTTTableEnum.mrogrid.getIndex()));
		CSList.add(new CSGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.gridcqt.getIndex()));
		CSList.add(new CSGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_DT ,MroCsOTTTableEnum.griddt.getIndex()));
		
		CSList.add(new CSTenGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_All ,MroCsOTTTableEnum.tenmrogrid.getIndex()));
		CSList.add(new CSTenGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.tengridcqt.getIndex()));
		CSList.add(new CSTenGridStatdo_4G(typeResult,StaticConfig.SOURCE_YD,StaticConfig.TestType_DT ,MroCsOTTTableEnum.tengriddt.getIndex()));
		 
 		List<HashMap> statMaps = Arrays.asList(new HashMap[]{new HashMap(),new HashMap(),new HashMap(),new HashMap(),new HashMap(),new HashMap(),new HashMap()});
		
			for (int i = 0; i < DT_Sample_4G.NCLTE_ARRAY_LENGTH; i++) 
			{
				CSList.add(new CSCellFreqStatdo_4G(i,typeResult,StaticConfig.SOURCE_YD,MroCsOTTTableEnum.mrocellfreq.getIndex()).withStatMap(statMaps.get(0)));
				CSList.add(new CSFreqCellDXStatdo_4G(i,typeResult,StaticConfig.SOURCE_YD, MroCsOTTTableEnum.DXfreqcellByImei.getIndex()).withStatMap(statMaps.get(1)));
				CSList.add(new CSFreqCellLTStatdo_4G(i,typeResult,StaticConfig.SOURCE_YD, MroCsOTTTableEnum.LTfreqcellByImei.getIndex()).withStatMap(statMaps.get(2)));
				
				CSList.add(new CSGridFreqStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.griddtfreq.getIndex()).withStatMap(statMaps.get(3)));
				CSList.add(new CSGridFreqStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.gridcqtfreq.getIndex()).withStatMap(statMaps.get(4)));
				CSList.add(new CSTenGridFreqStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.tengriddtfreq.getIndex()).withStatMap(statMaps.get(5)));
				CSList.add(new CSTenGridFreqStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.tengridcqtfreq.getIndex()).withStatMap(statMaps.get(6)));
//				CSList.add(new CSTenFreqGridLTStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.LTtenFreqByImeiDt.getIndex()));
//				CSList.add(new CSTenFreqGridLTStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.LTtenFreqByImeiCqt.getIndex()));
//				CSList.add(new CSTenFreqGridDXStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.DXtenFreqByImeiDt.getIndex()));
//				CSList.add(new CSTenFreqGridDXStatdo_4G(i,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.DXtenFreqByImeiCqt.getIndex()));
			}
			CSList.add(new CSFreqCellDXStatdo_4G(-1,typeResult,StaticConfig.SOURCE_YD, MroCsOTTTableEnum.DXfreqcellByImei.getIndex()));
			CSList.add(new CSFreqCellLTStatdo_4G(-1,typeResult,StaticConfig.SOURCE_YD, MroCsOTTTableEnum.LTfreqcellByImei.getIndex()));
			
//			CSList.add(new CSTenFreqGridLTStatdo_4G(-1,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.LTtenFreqByImeiDt.getIndex()));
//			CSList.add(new CSTenFreqGridLTStatdo_4G(-1,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.LTtenFreqByImeiCqt.getIndex()));
//			CSList.add(new CSTenFreqGridDXStatdo_4G(-1,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_DT,MroCsOTTTableEnum.DXtenFreqByImeiDt.getIndex()));
//			CSList.add(new CSTenFreqGridDXStatdo_4G(-1,typeResult, StaticConfig.SOURCE_YD,StaticConfig.TestType_CQT,MroCsOTTTableEnum.DXtenFreqByImeiCqt.getIndex()));
			// 手机支持联通或电信 却没有收到异频数据
		
		
		
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
