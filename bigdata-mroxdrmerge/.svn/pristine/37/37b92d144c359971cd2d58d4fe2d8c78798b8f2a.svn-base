package cn.mastercom.bigdata.pha.stat;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.BuildCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.BuildStatDo_4G;
import cn.mastercom.bigdata.mro.stat.CellStatDO_4G;
import cn.mastercom.bigdata.mro.stat.CompositeStatDo;
import cn.mastercom.bigdata.mro.stat.DayDataDeal_4G;
import cn.mastercom.bigdata.mro.stat.IStatDo;
import cn.mastercom.bigdata.mro.stat.InGridCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.InGridStatDo_4G;
import cn.mastercom.bigdata.mro.stat.MrSampleStatDo_4G;
import cn.mastercom.bigdata.mro.stat.OutGridCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.OutGridStatDo_4G;
import cn.mastercom.bigdata.util.ResultOutputer;

/**
 * @Description 数据处理
 * @date 20171208
 */
// TODO ①个人认为这里将具体的 统计类抽象出去(如OutGridStatDo_4G)，再分YD/LT/DX
// (OutGridStatDo_4G_YD)具体的处理类会更好，通过增加类来减少循环判断(单一职责原则)，减少状态传递，也方便管理。(PS:这里真正执行统计功能的是AStatDo的子类，而不是DayDataDeal_4G
// TODO ②typeResult参数仅在输出时用到，不必在构造函数中传入，增加类的耦合度
// TODO ③DayDataDeal_4G应该有一个上层的接口，而成员dealList的泛型应该是这个接口，那么到时候有变更的
// 如加入WeekDataDeal/MonthDataDeal/SeasonDataDeal就可以直接加入到成员dealList中，而不影响上下游结构
// TODO ④在mrstat包中应该分包(package)存放不同层次的接口/类，清晰结构，方便管理
public class PhaStatDeals
{
	private List<DayDataDeal_4G> dealList;

	/**
	 * 构造函数，会注入一些默认统计策略
	 * 
	 * @param typeResult
	 */
	@SuppressWarnings("rawtypes")
	public PhaStatDeals(ResultOutputer typeResult)
	{
		ArrayList<IStatDo> YDList = new ArrayList<IStatDo>();

		dealList = new ArrayList<DayDataDeal_4G>();

		// YD IN
		YDList.add(new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.IM,
				PhaEnum.DataType_PHA_INGRID_MID_YD.getMark()));

		YDList.add(new InGridCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.IM,
				PhaEnum.DataType_PHA_INGRID_CELL_MID.getMark()));

		YDList.add(new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.IM,
				PhaEnum.DataType_PHA_BUILDING_MID_YD.getMark()));

		YDList.add(new BuildCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.IM,
				PhaEnum.DataType_PHA_BUILDING_CELL_MID.getMark()));

		YDList.add(new CellStatDO_4G(typeResult, StaticConfig.SOURCE_YD, PhaEnum.DataType_PHA_CELL_YD.getMark()));

		YDList.add(new MrSampleStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.IM,
				PhaEnum.DataType_PHA_INSAMPLE_MID.getMark()));

		// YD OUT
		YDList.add(new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.OM, PhaEnum.DataType_PHA_OUTGRID_MID_YD.getMark()));

		YDList.add(new OutGridCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.OM, PhaEnum.DataType_PHA_OUTGRID_CELL_MID.getMark()));
		
		YDList.add(new MrSampleStatDo_4G(typeResult, StaticConfig.SOURCE_YD, StaticConfig.OM, PhaEnum.DataType_PHA_OUTSAMPLE_MID.getMark()));
		
		
		// 默认添加 YD,LT,DX
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(YDList)));
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
		for (DayDataDeal_4G deal : dealList)
		{
			deal.dealSample(sample);
		}
	}

	public void dealSample(DT_Sample_23G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
	}

	public int outResult()
	{
		for (DayDataDeal_4G deal : dealList)
		{
			deal.outResult();
		}

		return 0;
	}

}
