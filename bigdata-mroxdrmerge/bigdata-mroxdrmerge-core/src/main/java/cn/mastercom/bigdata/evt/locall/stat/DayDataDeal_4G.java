package cn.mastercom.bigdata.evt.locall.stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import cn.mastercom.bigdata.StructData.GridItemOfSize;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;

public class DayDataDeal_4G
{
	private Map<Integer, DayDataItem> dayDataDealMap;// time，天统计
	private int curDayTime;
	private ResultOutputer resultOutputer;

	public DayDataDeal_4G(ResultOutputer resultOutputer)
	{
		dayDataDealMap = new HashMap<Integer, DayDataItem>();
		this.resultOutputer = resultOutputer;
	}

	/**
	 * 按天做key，进行统计
	 * @param event
	 */
	public void dealEvent(EventData event)
	{
		if (event.iTime <= 0)
		{
			return;
		}

//		curDayTime = TimeHelper.getRoundDayTime(event.iTime);
		curDayTime = XdrLocallexDeal2.ROUND_dAY_TIME;
		DayDataItem dayDataDeal = dayDataDealMap.get(curDayTime);
		if (dayDataDeal == null)
		{
			dayDataDeal = new DayDataItem(resultOutputer, curDayTime, curDayTime + 86400);
			dayDataDealMap.put(curDayTime, dayDataDeal);
		}
		dayDataDeal.dealEvent(event);
	}

	public Map<Integer, DayDataItem> getDayDataDealMap()
	{
		return dayDataDealMap;
	}

	public int outResult()
	{
		for (DayDataItem item : dayDataDealMap.values())
		{

			item.outResult();
		}
		return 0;
	}

	public class DayDataItem
	{
		private int stime;
		private int etime;
		private ResultOutputer resultOutputer;

		private List<BaseStatDo> statdoList = new ArrayList<BaseStatDo>();

		private BaseStatDo stat_TB_EVENT_CELL;
		private BaseStatDo stat_TB_EVENT_IMSI;

		private BaseStatDo stat_TB_EVENT_HIGH_IN_CELLGRID;
		private BaseStatDo stat_TB_EVENT_MID_IN_CELLGRID;
		private BaseStatDo stat_TB_EVENT_LOW_IN_CELLGRID;

		private BaseStatDo stat_TB_EVENT_HIGH_IN_GRID;
		private BaseStatDo stat_TB_EVENT_MID_IN_GRID;
		private BaseStatDo stat_TB_EVENT_LOW_IN_GRID;

		private BaseStatDo stat_TB_EVENT_HIGH_OUT_CELLGRID;
		private BaseStatDo stat_TB_EVENT_MID_OUT_CELLGRID;
		private BaseStatDo stat_TB_EVENT_LOW_OUT_CELLGRID;

		private BaseStatDo stat_TB_EVENT_HIGH_OUT_GRID;
		private BaseStatDo stat_TB_EVENT_MID_OUT_GRID;
		private BaseStatDo stat_TB_EVENT_LOW_OUT_GRID;

		// 按照楼宇维度进行统计
		private BaseStatDo stat_TB_EVENT_HIGH_Build_CELLGRID;
		private BaseStatDo stat_TB_EVENT_MID_Build_CELLGRID;
		private BaseStatDo stat_TB_EVENT_LOW_Build_CELLGRID;
		
		//按照楼宇小区方位维度进行统计
		private BaseStatDo stat_TB_EVENT_HIGH_Build_CELL_POS;
		private BaseStatDo stat_TB_EVENT_MID_Build_CELL_POS;
		private BaseStatDo stat_TB_EVENT_LOW_Build_CELL_POS;

		private BaseStatDo stat_TB_EVENT_HIGH_Build_GRID;
		private BaseStatDo stat_TB_EVENT_MID_Build_GRID;
		private BaseStatDo stat_TB_EVENT_LOW_Build_GRID;
		
		//按照楼宇方位维度进行统计
		private BaseStatDo stat_TB_EVENT_HIGH_Build_POS;
		private BaseStatDo stat_TB_EVENT_MID_Build_POS;
		private BaseStatDo stat_TB_EVENT_LOW_Build_POS;
		
		//高铁场景统计
		private BaseStatDo stat_TB_EVENT_Area;
		private BaseStatDo stat_TB_EVENT_Area_GRID;
		private BaseStatDo stat_TB_EVENT_Area_CELLGRID;
		private BaseStatDo stat_TB_EVENT_Area_CELL;
		
		private BaseStatDo stat_TB_EVENT_HSR_GRID;
		private BaseStatDo stat_TB_EVENT_HSR_CELL_GRID;
		private BaseStatDo stat_TB_EVENT_HSR_CELL;
		private BaseStatDo stat_TB_EVENT_HSR_TRAINSEG;
		private BaseStatDo stat_TB_EVENT_HSR_TRAINSEG_CELL;
		
		

		public DayDataItem(ResultOutputer resultOutputer, int stime, int etime)
		{
			this.stime = stime;
			this.etime = etime;
			this.resultOutputer = resultOutputer;

			stat_TB_EVENT_CELL = new EventDataCellStat(stime, etime, TypeIoEvtEnum.CELLEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_CELL);
			stat_TB_EVENT_IMSI = new EventDataImsiStat(stime, etime, TypeIoEvtEnum.IMSIEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_IMSI);
			

			stat_TB_EVENT_HIGH_IN_CELLGRID = new EventDataInCellGridStat(stime, etime,TypeIoEvtEnum.IHCELLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_IN_CELLGRID);
			stat_TB_EVENT_MID_IN_CELLGRID = new EventDataInCellGridStat(stime, etime, TypeIoEvtEnum.IMCELLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_IN_CELLGRID);
			stat_TB_EVENT_LOW_IN_CELLGRID = new EventDataInCellGridStat(stime, etime, TypeIoEvtEnum.ILCELLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_IN_CELLGRID);

			stat_TB_EVENT_HIGH_IN_GRID = new EventDataInGridStat(stime, etime, TypeIoEvtEnum.IHGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_IN_GRID);
			stat_TB_EVENT_MID_IN_GRID = new EventDataInGridStat(stime, etime, TypeIoEvtEnum.IMGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_IN_GRID);
			stat_TB_EVENT_LOW_IN_GRID = new EventDataInGridStat(stime, etime, TypeIoEvtEnum.ILGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_IN_GRID);

			stat_TB_EVENT_HIGH_OUT_CELLGRID = new EventDataOutCellGridStat(stime, etime, TypeIoEvtEnum.OHCELLGRIDEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_OUT_CELLGRID);
			stat_TB_EVENT_MID_OUT_CELLGRID = new EventDataOutCellGridStat(stime, etime, TypeIoEvtEnum.OMCELLGRIDEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_OUT_CELLGRID);
			stat_TB_EVENT_LOW_OUT_CELLGRID = new EventDataOutCellGridStat(stime, etime, TypeIoEvtEnum.OLCELLGRIDEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_OUT_CELLGRID);

			stat_TB_EVENT_HIGH_OUT_GRID = new EventDataOutGridStat(stime, etime, TypeIoEvtEnum.OHGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_OUT_GRID);
			stat_TB_EVENT_MID_OUT_GRID = new EventDataOutGridStat(stime, etime, TypeIoEvtEnum.OMGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_OUT_GRID);
			stat_TB_EVENT_LOW_OUT_GRID = new EventDataOutGridStat(stime, etime, TypeIoEvtEnum.OLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_OUT_GRID);

			// build cell_grid
			stat_TB_EVENT_HIGH_Build_CELLGRID = new EventDataBuildCellGridStat(stime, etime, TypeIoEvtEnum.BHCELLGRIDEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_Build_CELLGRID);
			stat_TB_EVENT_MID_Build_CELLGRID = new EventDataBuildCellGridStat(stime, etime, TypeIoEvtEnum.BMCELLGRIDEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_Build_CELLGRID);
			stat_TB_EVENT_LOW_Build_CELLGRID = new EventDataBuildCellGridStat(stime, etime, TypeIoEvtEnum.BLCELLGRIDEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_Build_CELLGRID);
			
			//build cell position yzx add at 20180312
			stat_TB_EVENT_HIGH_Build_CELL_POS = new EventDataBuildCellPosStat(stime, etime, TypeIoEvtEnum.BHCELLPOSEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_Build_CELL_POS);
			stat_TB_EVENT_MID_Build_CELL_POS = new EventDataBuildCellPosStat(stime, etime, TypeIoEvtEnum.BMCELLPOSEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_Build_CELL_POS);
			stat_TB_EVENT_LOW_Build_CELL_POS = new EventDataBuildCellPosStat(stime, etime, TypeIoEvtEnum.BLCELLPOSEVT.getIndex(),
					resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_Build_CELL_POS);

			// build grid
			stat_TB_EVENT_HIGH_Build_GRID = new EventDataBuildGridStat(stime, etime, TypeIoEvtEnum.BHGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_Build_GRID);
			stat_TB_EVENT_MID_Build_GRID = new EventDataBuildGridStat(stime, etime, TypeIoEvtEnum.BMGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_Build_GRID);
			stat_TB_EVENT_LOW_Build_GRID = new EventDataBuildGridStat(stime, etime, TypeIoEvtEnum.BLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_Build_GRID);
			
			// build position yzx add at 20180312
			stat_TB_EVENT_HIGH_Build_POS = new EventDataBuildPosStat(stime, etime, TypeIoEvtEnum.BHPOSEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HIGH_Build_POS);
			stat_TB_EVENT_MID_Build_POS = new EventDataBuildPosStat(stime, etime, TypeIoEvtEnum.BMPOSEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_MID_Build_POS);
			stat_TB_EVENT_LOW_Build_POS = new EventDataBuildPosStat(stime, etime, TypeIoEvtEnum.BLPOSEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_LOW_Build_POS);
			
			//高铁场景
			/*stat_TB_EVENT_Area = new EventDataAreaStat(stime, etime, TypeIoEvt.AREAEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_Area);
			stat_TB_EVENT_Area_GRID = new EventDataAreaGridStat(stime, etime, TypeIoEvt.AREAGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_Area_GRID);
			stat_TB_EVENT_Area_CELLGRID = new EventDataAreaCellGridStat(stime, etime, TypeIoEvt.AREACELLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_Area_CELLGRID);
			stat_TB_EVENT_Area_CELL = new EventDataAreaCellStat(stime, etime, TypeIoEvt.AREACELLEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_Area_CELL);*/
			stat_TB_EVENT_HSR_GRID = new EventDataOutGridStat(stime, etime, TypeIoEvtEnum.HSRGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HSR_GRID);
			stat_TB_EVENT_HSR_CELL_GRID = new EventDataOutCellGridStat(stime, etime, TypeIoEvtEnum.HSRCELLGRIDEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HSR_CELL_GRID);
			stat_TB_EVENT_HSR_CELL = new EventDataCellStat(stime, etime, TypeIoEvtEnum.HSRCELLEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HSR_CELL);
			stat_TB_EVENT_HSR_TRAINSEG = new EventDataHsrTrainsegStat(stime, etime, TypeIoEvtEnum.HSRTRAINSEGEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HSR_TRAINSEG);
			stat_TB_EVENT_HSR_TRAINSEG_CELL = new EventDataHsrTrainsegCellStat(stime, etime, TypeIoEvtEnum.HSRTRAINSEGCELLEVT.getIndex(), resultOutputer);
			statdoList.add(stat_TB_EVENT_HSR_TRAINSEG_CELL);
		}

		public void outResult()
		{
			for (BaseStatDo item : statdoList)
			{
				if (item.outFinalReuslt() != 0)
				{
					// 这次不成功的话，应该有日志了吧
					LOGHelper.GetLogger().writeLog(LogType.error,
							"outFinalReuslt error: " + item.getClass().toString());
				}
			}
		}

		/**
		 * 通过定位出的室内室外，还是高低精度，都会去各自的实现类中进行统计
		 * @param event
		 */
		public void dealEvent(EventData event)
		{
			int indoorSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());
			int outdoorSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getOutDoorSize());

			if (event.confidentType == StaticConfig.OH)
			{

				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize);

				stat_TB_EVENT_HIGH_OUT_CELLGRID.stat(event);
				stat_TB_EVENT_HIGH_OUT_GRID.stat(event);

			}
			else if (event.confidentType == StaticConfig.OM)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize);

				stat_TB_EVENT_MID_OUT_CELLGRID.stat(event);
				stat_TB_EVENT_MID_OUT_GRID.stat(event);
			}
			else if (event.confidentType == StaticConfig.OL)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize);

				stat_TB_EVENT_LOW_OUT_CELLGRID.stat(event);
				stat_TB_EVENT_LOW_OUT_GRID.stat(event);
			}
			else if (event.confidentType == StaticConfig.IH)
			{

				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, indoorSize);

				stat_TB_EVENT_HIGH_IN_CELLGRID.stat(event);
				stat_TB_EVENT_HIGH_IN_GRID.stat(event);

				stat_TB_EVENT_HIGH_Build_GRID.stat(event);
				stat_TB_EVENT_HIGH_Build_CELLGRID.stat(event);
				
				stat_TB_EVENT_HIGH_Build_POS.stat(event);
				stat_TB_EVENT_HIGH_Build_CELL_POS.stat(event);

			}
			else if (event.confidentType == StaticConfig.IM)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, indoorSize);

				stat_TB_EVENT_MID_IN_CELLGRID.stat(event);
				stat_TB_EVENT_MID_IN_GRID.stat(event);

				stat_TB_EVENT_MID_Build_GRID.stat(event);
				stat_TB_EVENT_MID_Build_CELLGRID.stat(event);
				
				stat_TB_EVENT_MID_Build_POS.stat(event);
				stat_TB_EVENT_MID_Build_CELL_POS.stat(event);
			}
			else if (event.confidentType == StaticConfig.IL)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, indoorSize);

				stat_TB_EVENT_LOW_IN_CELLGRID.stat(event);
				stat_TB_EVENT_LOW_IN_GRID.stat(event);

				stat_TB_EVENT_LOW_Build_GRID.stat(event);
				stat_TB_EVENT_LOW_Build_CELLGRID.stat(event);
				
				stat_TB_EVENT_LOW_Build_POS.stat(event);
				stat_TB_EVENT_LOW_Build_CELL_POS.stat(event);
			}

			stat_TB_EVENT_CELL.stat(event);
			stat_TB_EVENT_IMSI.stat(event);
			
			//高铁场景统计
			if(event.iTestType == StaticConfig.TestType_HiRail){

				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize); 
//				stat_TB_EVENT_Area.stat(event);
//				stat_TB_EVENT_Area_GRID.stat(event);
//				stat_TB_EVENT_Area_CELLGRID.stat(event);
//				stat_TB_EVENT_Area_CELL.stat(event);
				
				stat_TB_EVENT_HSR_GRID.stat(event);
				stat_TB_EVENT_HSR_CELL_GRID.stat(event);
				stat_TB_EVENT_HSR_CELL.stat(event);
				stat_TB_EVENT_HSR_TRAINSEG.stat(event);
				stat_TB_EVENT_HSR_TRAINSEG_CELL.stat(event);
			}

		}
	}

}
