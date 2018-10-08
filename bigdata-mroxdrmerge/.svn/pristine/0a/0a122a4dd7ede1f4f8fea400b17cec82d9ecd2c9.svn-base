package cn.mastercom.bigdata.mro.loc;

import java.util.Map;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StatFreqGrid;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_4G;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.xdr.loc.CellGridData_4G;
import cn.mastercom.bigdata.xdr.loc.DayDataDeal_23G;
import cn.mastercom.bigdata.xdr.loc.DayDataDeal_4G;
import cn.mastercom.bigdata.xdr.loc.GridData_4G;
import cn.mastercom.bigdata.xdr.loc.GridData_Freq;
import cn.mastercom.bigdata.xdr.loc.HourDataDeal_4G;
import cn.mastercom.bigdata.xdr.loc.ResultHelper;

public class StatDeal_CQT extends cn.mastercom.bigdata.xdr.loc.StatDeal_CQT
{

	public StatDeal_CQT(ResultOutputer resultOutputer)
	{
		super(resultOutputer);

		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_CQT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_CQT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_CQT);
	}

	public StatDeal_CQT(ResultOutputer resultOutputer, int type)
	{
		super(resultOutputer);
		data_type = type;
		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_CQT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_CQT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_CQT);
	}

	@Override
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
						String value = ResultHelper.getPutGrid_4G(gridData.getLteGrid());
						resultOutputer.pushData(MroCsOTTTableEnum.gridcqt.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();

				for (GridData_4G gridData : gridTimeDeal.getTen_gridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						String value = ResultHelper.getPutGrid_4G(gridData.getLteGrid());
						resultOutputer.pushData(MroCsOTTTableEnum.tengridcqt.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_gridDataMap().clear();
			}
		}

		// 输出栅格统计结果
		if (dayDataDeal_4G.getCellGridCount() > 10000)
		{
			for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getGridDataFreqMap().values())
				{
					for (GridData_Freq gridData : gridFreqMap.values())
					{
						gridData.finalDeal();
						try
						{
							String value = ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid());
							resultOutputer.pushData(MroCsOTTTableEnum.gridcqtfreq.getIndex(), value);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				gridTimeDeal.getGridDataFreqMap().clear();
				for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getTen_gridDataFreqMap().values())
				{
					for (GridData_Freq gridData : gridFreqMap.values())
					{
						gridData.finalDeal();
						try
						{
							String value = ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid());
							resultOutputer.pushData(MroCsOTTTableEnum.tengridcqtfreq.getIndex(), value);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				gridTimeDeal.getTen_gridDataFreqMap().clear();

				// new freqgrid byImei
				for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_LTfreqGridMap().values())
				{
					try
					{
						resultOutputer.pushData(MroCsOTTTableEnum.LTtenFreqByImeiCqt.getIndex(), gridFreqMap.toLine());
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_LTfreqGridMap().clear();

				for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_DXfreqGridMap().values())
				{
					try
					{
						resultOutputer.pushData(MroCsOTTTableEnum.DXtenFreqByImeiCqt.getIndex(), gridFreqMap.toLine());
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_DXfreqGridMap().clear();

				////
				// 输出cqt小区栅格数据

				for (Map.Entry<Long, CellGridData_4G> valuePare : gridTimeDeal.getTen_cellGridDataMap().entrySet())
				{
					valuePare.getValue().finalDeal();

					for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
					{
						try
						{
							String value = ResultHelper.getPutCellGrid_4G(lteCellGrid);
							resultOutputer.pushData(MroCsOTTTableEnum.tencellgridcqt.getIndex(), value);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				gridTimeDeal.getTen_cellGridDataMap().clear();
			}
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}

	@Override
	public void outAllData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					String value = ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid());
					resultOutputer.pushData(MroCsOTTTableEnum.gridcqt.getIndex(), value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getGridDataMap().clear();

			for (GridData_4G gridData : gridTimeDeal.getTen_gridDataMap().values())
			{
				gridData.finalDeal();
				try
				{
					String value = ResultHelper.getPutGrid_4G(gridData.getLteGrid());
					resultOutputer.pushData(MroCsOTTTableEnum.tengridcqt.getIndex(), value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_gridDataMap().clear();
		}

		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getGridDataFreqMap().values())
			{
				for (GridData_Freq gridData : gridFreqMap.values())
				{
					gridData.finalDeal();
					try
					{
						String value = ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid());
						resultOutputer.pushData(MroCsOTTTableEnum.gridcqtfreq.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}

			}
			gridTimeDeal.getGridDataFreqMap().clear();
			for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getTen_gridDataFreqMap().values())
			{
				for (GridData_Freq gridData : gridFreqMap.values())
				{
					gridData.finalDeal();
					try
					{
						String value = ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid());
						resultOutputer.pushData(MroCsOTTTableEnum.tengridcqtfreq.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}

			}
			gridTimeDeal.getTen_gridDataFreqMap().clear();

			// new freqgrid byImei
			for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_LTfreqGridMap().values())
			{
				try
				{
					resultOutputer.pushData(MroCsOTTTableEnum.LTtenFreqByImeiCqt.getIndex(), gridFreqMap.toLine());
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_LTfreqGridMap().clear();

			for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_DXfreqGridMap().values())
			{
				try
				{
					resultOutputer.pushData(MroCsOTTTableEnum.DXtenFreqByImeiCqt.getIndex(), gridFreqMap.toLine());
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_DXfreqGridMap().clear();

			////
			// 输出cqt小区栅格数据

			for (Map.Entry<Long, CellGridData_4G> valuePare : gridTimeDeal.getTen_cellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						String value = ResultHelper.getPutCellGrid_4G(lteCellGrid);
						resultOutputer.pushData(MroCsOTTTableEnum.tencellgridcqt.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
			gridTimeDeal.getTen_cellGridDataMap().clear();
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}

}
