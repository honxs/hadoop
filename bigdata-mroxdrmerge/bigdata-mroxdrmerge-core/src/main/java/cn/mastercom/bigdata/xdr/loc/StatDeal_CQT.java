package cn.mastercom.bigdata.xdr.loc;

import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;

public class StatDeal_CQT extends StatDeal
{
	public StatDeal_CQT(ResultOutputer resultOutputer)
	{
		super(resultOutputer);
		
		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_CQT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_CQT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_CQT);
	}
	
	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		if(sample.testType != StaticConfig.TestType_CQT)
		{
            return;
		}

		// 天统计
		dayDataDeal_4G.dealSample(sample);
	}
	
	public void dealSample(DT_Sample_23G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		if(sample.testType != StaticConfig.TestType_CQT)
		{
            return;
		}

		// 天统计
		dayDataDeal_23G.dealSample(sample);
	}
	
	public void outDealingData()
	{
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
		// 输出栅格统计结果
		if (dayDataDeal_4G.getGridCount() > 10000)
		{
			for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (GridData_4G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						String value = ResultHelper.getPutGrid_4G(gridData.getLteGrid());
						resultOutputer.pushData(XdrLocTablesEnum.xdrgridcqt.getIndex(), value);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal_CQT.outXdrgridcqt error",
								"xdrloc StatDeal_CQT.outXdrgridcqt error",	e);
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}
		
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////
		
		// 输出栅格统计结果
		if (dayDataDeal_23G.getGridCount() > 10000)
		{
			for (DayDataDeal_23G.DayDataItem gridTimeDeal : dayDataDeal_23G.getDayDataDealMap().values())
			{
				for (GridData_23G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
//						curText.set(ResultHelper.getPutGrid_23G(gridData.getGridItem()));
						String value = ResultHelper.getPutGrid_23G(gridData.getGridItem());
						resultOutputer.pushData(XdrLocTablesEnum.xdrgridcqt23g.getIndex(), value);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal_CQT.xdrgridcqt23g error",
								"xdrloc StatDeal_CQT.xdrgridcqt23g error",	e);
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////
		
	}

	@Override
	public void outAllData()
	{
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					String value = ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid());
					resultOutputer.pushData(XdrLocTablesEnum.xdrgridcqt.getIndex(), value);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal_CQT.outXdrgridcqt error",
							"xdrloc StatDeal_CQT.outXdrgridcqt error",	e);
				}
			}
			gridTimeDeal.getGridDataMap().clear();
		}
		
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////
		
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_23G.DayDataItem gridTimeDeal : dayDataDeal_23G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_23G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					String value = ResultHelper.getPutGrid_23G(valuePare.getValue().getGridItem());
					resultOutputer.pushData(XdrLocTablesEnum.xdrgridcqt23g.getIndex(), value);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal_CQT.outXdrgridcqt23g error",
							"xdrloc StatDeal_CQT.outXdrgridcqt23g error",	e);
				}
			}
			gridTimeDeal.getGridDataMap().clear();
		}
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////
		
	}
	
	
}