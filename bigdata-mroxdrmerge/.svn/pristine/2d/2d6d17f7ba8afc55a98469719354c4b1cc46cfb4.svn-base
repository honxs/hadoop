package cn.mastercom.bigdata.mro.loc.hsr.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.CompositeStatDo;
import cn.mastercom.bigdata.mro.stat.DayDataDeal_4G;
import cn.mastercom.bigdata.mro.stat.IStatDo;
import cn.mastercom.bigdata.mro.stat.OutGridCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.OutGridStatDo_4G;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.util.ResultOutputer;

public class StatDeal_HSR
{
private List<DayDataDeal_4G> dealList;
	
	/**
	 * 构造函数，会注入一些默认统计策略
	 * @param typeResult
	 */
	public StatDeal_HSR(ResultOutputer typeResult)
	{
			ArrayList<IStatDo> YDList = new ArrayList<IStatDo>();
			ArrayList<IStatDo> LTList = new ArrayList<IStatDo>();
			ArrayList<IStatDo> DXList = new ArrayList<IStatDo>();
			dealList = new ArrayList<DayDataDeal_4G>();
			// YD
			// 栅格统计
			YDList.add(new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD, MroBsTablesEnum.mroHsrYdGrid.getIndex()));
			// 小区栅格统计
			YDList.add(new OutGridCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD, MroBsTablesEnum.mroHsrYdGridCell.getIndex()));
			// 小区统计
			YDList.add(new HsrCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD, MroBsTablesEnum.mroHsrYdCell.getIndex()));
			// 覆盖采样点
			YDList.add(new HsrMrSampleStatDo_4G(typeResult, StaticConfig.SOURCE_YD, MroBsTablesEnum.mroHsrSample.getIndex()));
			//TODO 车次路段无覆盖
			// 车次路段
			YDList.add(new TrainSegStatDo_4G(typeResult, StaticConfig.SOURCE_YD, MroBsTablesEnum.mroHsrYdTrainSeg.getIndex()));
			// 车次路段小区
			YDList.add(new TrainSegCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD, MroBsTablesEnum.mroHsrYdTrainSegCell.getIndex()));
			
			//LT
			// 栅格统计
			LTList.add(new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT, MroBsTablesEnum.mroHsrLtGrid.getIndex()));
			//TODO 车次路段无覆盖
			// 车次路段
			LTList.add(new TrainSegStatDo_4G(typeResult, StaticConfig.SOURCE_LT, MroBsTablesEnum.mroHsrLtTrainSeg.getIndex()));
			
			//DX
			// 栅格统计
			DXList.add(new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_DX, MroBsTablesEnum.mroHsrDxGrid.getIndex()));
			//TODO 车次路段无覆盖
			// 车次路段
			DXList.add(new TrainSegStatDo_4G(typeResult, StaticConfig.SOURCE_DX, MroBsTablesEnum.mroHsrDxTrainSeg.getIndex()));
			
			// 默认添加 YD,LT,DX
			dealList.add(new DayDataDeal_4G(new CompositeStatDo(YDList)));
			dealList.add(new DayDataDeal_4G(new CompositeStatDo(LTList)));
			dealList.add(new DayDataDeal_4G(new CompositeStatDo(DXList)));
	}
	/**
	 * 构造函数，调用此构造函数需要注入统计策略，把策略暴露给调用者
	 * @param typeResult
	 * @param dataDeals 
	 */
	public StatDeal_HSR(ResultOutputer typeResult, DayDataDeal_4G...dataDeals )
	{
		dealList = new ArrayList<>(Arrays.asList(dataDeals));
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
		for(DayDataDeal_4G deal : dealList){
			deal.dealSample(sample);
		}
	}

	public int outResult()
	{
		for(DayDataDeal_4G deal : dealList){
			deal.outResult();
		}

		return 0;
	}

}
