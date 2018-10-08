package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_23G;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_4G;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.data.MyInt;
import cn.mastercom.bigdata.xdr.loc.HourDataDeal_4G.HourDataItem;

public class StatDeal
{
	public static final int STATDEAL_DT = 1;
	public static final int STATDEAL_CQT = 2;
	public static final int STATDEAL_ALL = 3;

	protected ResultOutputer resultOutputer;
	protected Text curText = new Text();

	protected DayDataDeal_4G dayDataDeal_4G;// time，天统计
	protected DayDataDeal_23G dayDataDeal_23G;// time，天统计
	protected HourDataDeal_4G hourDataDeal_4G;
	protected int curHourTime;
	protected int curDayTime;
	// 处理数据类型
	protected int data_type;

	public Map<Long, MyInt> cellLocDic = new HashMap<Long, MyInt>();

	public StatDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_ALL);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_ALL);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_ALL);
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
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

		// 天统计
		dayDataDeal_23G.dealSample(sample);
	}
	
	public void outDealingData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

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
						resultOutputer.pushData(XdrLocTablesEnum.xdrgrid.getIndex(), ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrGrid error",
								"xdrloc StatDeal.outXdrGrid error",	e);
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}

		// 输出小区栅格
		if (dayDataDeal_4G.getCellGridCount() > 10000)
		{
			for (DayDataDeal_4G.DayDataItem dayDataDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (CellGridData_4G cellGridData : dayDataDeal.getCellGridDataMap().values())
				{
					cellGridData.finalDeal();
					for (Stat_CellGrid_4G lteCellGrid : cellGridData.getGridDataMap().values())
					{
						try
						{
//							curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
							resultOutputer.pushData(XdrLocTablesEnum.xdrcellgrid.getIndex(), ResultHelper.getPutCellGrid_4G(lteCellGrid));
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc outXdrCellgrid error",
									"xdrloc outXdrCellgrid error, the xdrcellgrid.getIndex() is "
									+XdrLocTablesEnum.xdrcellgrid.getIndex(),	e);
						}
					}

				}
				dayDataDeal.getCellGridDataMap().clear();
			}
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

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
						resultOutputer.pushData(XdrLocTablesEnum.xdrgrid23g.getIndex(), ResultHelper.getPutGrid_23G(gridData.getGridItem()));
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal outXdrgrid23G error",
								"xdrloc StatDeal outXdrgrid23G error",	e);
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}

		// 输出小区栅格
		if (dayDataDeal_23G.getCellGridCount() > 10000)
		{
			for (DayDataDeal_23G.DayDataItem dayDataDeal : dayDataDeal_23G.getDayDataDealMap().values())
			{
				for (CellGridData_23G cellGridData : dayDataDeal.getCellGridDataMap().values())
				{
					cellGridData.finalDeal();
					for (Stat_CellGrid_23G lteCellGrid : cellGridData.getGridCellGridMap().values())
					{
						try
						{
//							curText.set(ResultHelper.getPutCellGrid_23G(lteCellGrid));
							resultOutputer.pushData(XdrLocTablesEnum.xdrcellgrid23g.getIndex(), ResultHelper.getPutCellGrid_23G(lteCellGrid));
						}
						catch (Exception e)
						{
                            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrCellGrid23G error",
                                    "xdrloc StatDeal.outXdrCellGrid23G error",	e);
						}
					}

				}
				dayDataDeal.getCellGridDataMap().clear();
			}
		}

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}

	public void outAllData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 小时数据吐出/////////////////////////////////////////////////////////////////////////////////////
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					String value = ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid());
					resultOutputer.pushData(XdrLocTablesEnum.xdrgrid.getIndex(), value);
				}
				catch (Exception e)
				{
                    LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrgrid error",
                            "xdrloc StatDeal.outXdrgrid error",	e);
				}
			}
			gridTimeDeal.getGridDataMap().clear();
		}

		// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem dayDataDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			// 输出小区天数据
			for (Map.Entry<Long, CellData_4G> valuePare : dayDataDeal.getCellDataMap().entrySet())
			{
				try
				{
					if (cellLocDic.containsKey(valuePare.getKey()))
					{
						valuePare.getValue().getLteCell().origLocXdrCount = cellLocDic.get(valuePare.getKey()).data;
					}

					String value = ResultHelper.getPutCell_4G(valuePare.getValue().getLteCell());
					resultOutputer.pushData(XdrLocTablesEnum.xdrcell.getIndex(), value);
				}
				catch (Exception e)
				{
                    LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrcell error",
                            "xdrloc StatDeal.outXdrcell error",	e);
				}
			}
			dayDataDeal.getCellDataMap().clear();
			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						resultOutputer.pushData(XdrLocTablesEnum.xdrcellgrid.getIndex(), ResultHelper.getPutCellGrid_4G(lteCellGrid));
					}
					catch (Exception e)
					{
                        LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrCellGrid error",
                                "xdrloc StatDeal.outXdrCellGrid error",	e);
					}
				}
			}
			dayDataDeal.getCellGridDataMap().clear();
		}
		
		for (HourDataItem hourData : hourDataDeal_4G.getHourDataDealMap().values())
		{
			for (Map.Entry<GridItem, UserGridStat_4G> valuePare : hourData.getUserGirdDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					String value = ResultHelper.getPutUserGridInfo(valuePare.getValue().getUserGrid());
					resultOutputer.pushData(XdrLocTablesEnum.xdrgriduserhour.getIndex(), value);
				}
				catch (Exception e)
				{
                    LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrgriduserhour error",
                            "xdrloc StatDeal.outXdrgriduserhour error",	e);
				}
			}
			hourData.getUserGirdDataMap().clear();

		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		for (DayDataDeal_23G.DayDataItem gridTimeDeal : dayDataDeal_23G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_23G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					String value = ResultHelper.getPutGrid_23G(valuePare.getValue().getGridItem());
					resultOutputer.pushData(XdrLocTablesEnum.xdrgrid23g.getIndex(), value);
				}
				catch (Exception e)
				{
                    LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrGrid23G error",
                            "xdrloc StatDeal.outXdrGrid23G error",	e);
				}
			}
			gridTimeDeal.getGridDataMap().clear();
		}

		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_23G.DayDataItem dayDataDeal : dayDataDeal_23G.getDayDataDealMap().values())
		{

			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_23G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_23G lteCellGrid : valuePare.getValue().getGridCellGridMap().values())
				{
					try
					{
						String value = ResultHelper.getPutCellGrid_23G(lteCellGrid);
						resultOutputer.pushData(XdrLocTablesEnum.xdrcellgrid23g.getIndex(), value);
					}
					catch (Exception e)
					{
                        LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc StatDeal.outXdrCellGrid23G error",
                                "xdrloc StatDeal.outXdrCellGrid23G error",	e);
					}
				}
			}
			dayDataDeal.getCellGridDataMap().clear();
		}

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}
}
