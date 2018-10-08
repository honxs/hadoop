package cn.mastercom.bigdata.mro.loc;

import java.util.Map;

import cn.mastercom.bigdata.StructData.CellFreqItem;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StatFreqCell;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_4G;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.xdr.loc.CellData_4G;
import cn.mastercom.bigdata.xdr.loc.CellData_Freq;
import cn.mastercom.bigdata.xdr.loc.CellGridData_4G;
import cn.mastercom.bigdata.xdr.loc.DayDataDeal_23G;
import cn.mastercom.bigdata.xdr.loc.DayDataDeal_4G;
import cn.mastercom.bigdata.xdr.loc.GridData_4G;
import cn.mastercom.bigdata.xdr.loc.HourDataDeal_4G;
import cn.mastercom.bigdata.xdr.loc.ResultHelper;

public class FgStatDeal extends cn.mastercom.bigdata.xdr.loc.StatDeal
{

	public FgStatDeal(ResultOutputer resultOutputer)
	{
		super(resultOutputer);

		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_ALL);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_ALL);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_ALL);
	}

	@Override
	public void outDealingData()
	{
		/////////////////////////////////////////////// 4G 输出栅格统计结果
		if (dayDataDeal_4G.getGridCount() > 100000)
		{
			for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (GridData_4G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						String value = ResultHelper.getPutGrid_4G(gridData.getLteGrid());
						resultOutputer.pushData(MroCsFgTableEnum.fpmrogrid.getIndex(), value);
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
						resultOutputer.pushData(MroCsFgTableEnum.fptenmrogrid.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_gridDataMap().clear();
			}
		}

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
					resultOutputer.pushData(MroCsFgTableEnum.fpmrogrid.getIndex(), value);
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
					resultOutputer.pushData(MroCsFgTableEnum.fptenmrogrid.getIndex(), value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_gridDataMap().clear();
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
					resultOutputer.pushData(MroCsFgTableEnum.fpmrocell.getIndex(), value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			dayDataDeal.getCellDataMap().clear();
			// 输出异频小区天数据
			for (Map.Entry<CellFreqItem, CellData_Freq> valuePare : dayDataDeal.getCellDataFreqMap().entrySet())
			{
				try
				{
					String value = ResultHelper.getPutCell_Freq(valuePare.getValue().getLteCell());
					resultOutputer.pushData(MroCsFgTableEnum.fpmrocellfreq.getIndex(), value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			dayDataDeal.getCellDataFreqMap().clear();
			// 输出new freqCell byImei
			for (StatFreqCell valuePare : dayDataDeal.getFreqLTCellMap().values())
			{
				try
				{
					resultOutputer.pushData(MroCsFgTableEnum.fpLTfreqcellByImei.getIndex(), valuePare.toLine());
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			dayDataDeal.getFreqLTCellMap().clear();
			for (StatFreqCell valuePare : dayDataDeal.getFreqDXCellMap().values())
			{
				try
				{
					resultOutputer.pushData(MroCsFgTableEnum.fpDXfreqcellByImei.getIndex(), valuePare.toLine());
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			dayDataDeal.getFreqDXCellMap().clear();
			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						String value = ResultHelper.getPutCellGrid_4G(lteCellGrid);
						resultOutputer.pushData(MroCsFgTableEnum.fpmrocellgrid.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
			dayDataDeal.getCellGridDataMap().clear();
			for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getTen_cellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						String value = ResultHelper.getPutCellGrid_4G(lteCellGrid);
						resultOutputer.pushData(MroCsFgTableEnum.fptenmrocellgrid.getIndex(), value);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
			dayDataDeal.getTen_cellGridDataMap().clear();
		}
	}
}
