package cn.mastercom.bigdata.stat.village;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.GridItemOfSize;
import cn.mastercom.bigdata.StructData.LteScPlrQciData;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.Stat_Grid_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.config.ImeiConfig;
import cn.mastercom.bigdata.mro.loc.MroLocStat;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.LteStatHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.xdr.loc.ResultHelper;

public class MroVillageDeal implements IDataDeal
{
	public static final int mrosample = "mrosample".hashCode();
	public static final int mrogrid = "mrogrid".hashCode();
	
	public static final int DataType_VillageGrid = 1;
	public static final int DataType_Mro = 100;
	
	private int villageLongitude;
	private int villageLatitude;
	private int IndoorGridSize;
	private int OutdoorGridSize;
	private ArrayList<SIGNAL_MR_All> mroItemList;
	
	private ResultOutputer resultOutputer;
	public MroVillageDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
		this.IndoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());
		this.OutdoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getOutDoorSize());
		villageLongitude = 0;
		villageLatitude = 0;
	}
	
	public void init()
	{
		mroItemList = new ArrayList<>();
	}
	
	// 吐出用户过程数据，为了防止内存过多
	private void outDealingData(List<SIGNAL_MR_All> mroItemList)
	{
		dealSample(mroItemList);
	}

	private void dealSample(List<SIGNAL_MR_All> mroList)
	{
		if(mroList.size() <= 0)
		{
			return;
		}

		DT_Sample_4G sample = new DT_Sample_4G();
		Map<Integer, Stat_Grid_4G> lteGridMap = new HashMap<Integer, Stat_Grid_4G>();
		for (SIGNAL_MR_All data : mroList)
		{
			sample.Clear();

			statMro(sample, data);		
			statKpi(sample, lteGridMap);
		}
		
		try
		{
			for(Stat_Grid_4G item : lteGridMap.values())
			{
				resultOutputer.pushData(mrogrid, ResultHelper.getPutGrid_4G(item));
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"mrovilagedeal write error", "mrovilagedeal write error : ", e);
		}
	}

	private void statKpi(DT_Sample_4G sample, Map<Integer, Stat_Grid_4G> lteGridMap)
	{
		Stat_Grid_4G lteGrid = lteGridMap.get(sample.cityID);
		if(lteGrid == null)
		{
		    lteGrid = new Stat_Grid_4G();
			GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
			lteGrid.startTime = TimeHelper.getRoundDayTime(sample.itime);
			lteGrid.endTime = lteGrid.startTime + 86400;
			lteGrid.icityid = sample.cityID;
			lteGrid.itllongitude = gridItem.getTLLongitude();
			lteGrid.itllatitude = gridItem.getTLLatitude();
			lteGrid.ibrlongitude = gridItem.getBRLongitude();
			lteGrid.ibrlatitude = gridItem.getBRLatitude();
			
			lteGridMap.put(sample.cityID, lteGrid);
		}
		
		lteGrid.isamplenum++;
		lteGrid.MrCount++;		
		LteStatHelper.statMro(sample, lteGrid.tStat);
	}
	
	private void statMro(DT_Sample_4G tsam, SIGNAL_MR_All tTemp)
	{
		if (tTemp.ibuildingID > 0)
		{
			tsam.ibuildingID = tTemp.ibuildingID;
		}
		else
		{
			tsam.ibuildingID = -1;
		}
		if (tTemp.iheight < 0)
		{
			tsam.iheight = -1;
		}
		else
		{
			tsam.iheight = tTemp.iheight;
		}
		tsam.simuLatitude = tTemp.simuLatitude;
		tsam.simuLongitude = tTemp.simuLongitude;
		tsam.testType = tTemp.testType;
		tsam.samState = tTemp.samState;
		tsam.locSource = tTemp.locSource;
		tsam.cityID = tTemp.tsc.cityID;
		tsam.itime = tTemp.tsc.beginTime;
		tsam.wtimems = (short) (tTemp.tsc.beginTimems);
		tsam.ilongitude = tTemp.tsc.longitude;
		tsam.ilatitude = tTemp.tsc.latitude;
		tsam.IMSI = tTemp.tsc.IMSI;
		tsam.UETac = tTemp.UETac;
		tsam.iLAC = (int) MroLocStat.getValidData(tsam.iLAC, tTemp.tsc.TAC);
		tsam.iCI = (long) MroLocStat.getValidData(tsam.iCI, tTemp.tsc.CellId);
		tsam.Eci = (long) MroLocStat.getValidData(tsam.Eci, tTemp.tsc.Eci);
		tsam.eventType = 0;
		tsam.ENBId = (int) MroLocStat.getValidData(tsam.ENBId, tTemp.tsc.ENBId);
		tsam.UserLabel = tTemp.tsc.UserLabel;
		tsam.wifilist = tTemp.tsc.UserLabel;// mrall 中userlabel中装的wifi信息
		tsam.CellId = (long) MroLocStat.getValidData(tsam.CellId, tTemp.tsc.CellId);
		tsam.Earfcn = tTemp.tsc.Earfcn;
		tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
		tsam.MmeCode = (int) MroLocStat.getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
		tsam.MmeGroupId = (int) MroLocStat.getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
		tsam.MmeUeS1apId = (long) MroLocStat.getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
		tsam.Weight = tTemp.tsc.Weight;
		tsam.LteScRSRP = tTemp.tsc.LteScRSRP;
		tsam.LteScRSRQ = tTemp.tsc.LteScRSRQ;
		tsam.LteScEarfcn = tTemp.tsc.LteScEarfcn;
		tsam.LteScPci = tTemp.tsc.LteScPci;
		tsam.LteScBSR = tTemp.tsc.LteScBSR;
		tsam.LteScRTTD = tTemp.tsc.LteScRTTD;
		tsam.LteScTadv = tTemp.tsc.LteScTadv;
		tsam.LteScAOA = tTemp.tsc.LteScAOA;
		tsam.LteScPHR = tTemp.tsc.LteScPHR;
		tsam.LteScRIP = tTemp.tsc.LteScRIP;
		tsam.LteScSinrUL = tTemp.tsc.LteScSinrUL;
		tsam.LocFillType = 1;

		tsam.testType = tTemp.testType;
		tsam.location = tTemp.location;
		tsam.dist = tTemp.dist;
		tsam.radius = tTemp.radius;
		tsam.locType = tTemp.locType;
		tsam.indoor = tTemp.indoor;
		tsam.networktype = tTemp.networktype;
		tsam.label = tTemp.label;

		tsam.serviceType = tTemp.serviceType;
		tsam.serviceSubType = tTemp.subServiceType;

		tsam.moveDirect = tTemp.moveDirect;

		tsam.LteScPUSCHPRBNum = tTemp.tsc.LteScPUSCHPRBNum;
		tsam.LteScPDSCHPRBNum = tTemp.tsc.LteScPDSCHPRBNum;
		tsam.imeiTac = tTemp.tsc.imeiTac;
		tsam.fullNetType = ImeiConfig.GetInstance().getValue(tTemp);
		tsam.eciSwitchList = tTemp.eciSwitchList;
		tsam.ConfidenceType = tTemp.ConfidenceType;
		if (tsam.samState == StaticConfig.ACTTYPE_OUT || tsam.iAreaType > 0)
		{
			tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, OutdoorGridSize);
		}
		else if (tsam.samState == StaticConfig.ACTTYPE_IN)
		{
			tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, IndoorGridSize);
		}
		tsam.MSISDN = tTemp.tsc.Msisdn;

		if (tTemp.tsc.EventType.equals("MRO"))
		{
			tsam.flag = "MRO";
		}
		else if (tTemp.tsc.EventType.equals("MDT_IMM"))
		{
			tsam.flag = "MDT_IMM";
		}
		else if (tTemp.tsc.EventType.equals("MDT_LOG"))
		{
			tsam.flag = "MDT_LOG";
		}
		else
		{
			tsam.flag = "MRO";
		}
		tsam.mrType = tTemp.tsc.EventType;

		for (int i = 0; i < tsam.nccount.length; i++)
		{
			tsam.nccount[i] = tTemp.nccount[i];
		}

		for (int i = 0; i < tsam.tlte.length; i++)
		{
			tsam.tlte[i] = tTemp.tlte[i];
			if (tTemp.tlte[i].LteNcRSRP >= -150 && tTemp.tlte[i].LteNcRSRP <= -30)
			{
				if (tTemp.tlte[i].LteNcRSRP >= tTemp.tsc.LteScRSRP - 6)
				{
					if (tTemp.tlte[i].LteNcEarfcn == tTemp.tsc.LteScEarfcn)
					{
						tsam.overlapSameEarfcn++;
					}
					tsam.OverlapAll++;
				}
			}
		}

//		for (int i = 0; i < tsam.ttds.length; i++)
//		{
//			tsam.ttds[i] = tTemp.ttds[i];
//		}

		for (int i = 0; i < tsam.tgsm.length; i++)
		{
			tsam.tgsm[i] = tTemp.tgsm[i];
		}

//		for (int i = 0; i < tsam.trip.length; i++)
//		{
//			tsam.trip[i] = tTemp.trip[i];
//		}
		
		for (int i = 0; i < tsam.dx_freq.length; i++)
		{
			tsam.dx_freq[i] = tTemp.dx_freq[i];
		}
		
		for (int i = 0; i < tsam.lt_freq.length; i++)
		{
			tsam.lt_freq[i] = tTemp.lt_freq[i];
		}
		
//		tsam.LteScRSRP_DX = tTemp.LteScRSRP_DX;
//		tsam.LteScRSRQ_DX = tTemp.LteScRSRQ_DX;
//		tsam.LteScEarfcn_DX = tTemp.LteScEarfcn_DX;
//		tsam.LteScPci_DX = tTemp.LteScPci_DX;
//		tsam.LteScRSRP_LT = tTemp.LteScRSRP_LT;
//		tsam.LteScRSRQ_LT = tTemp.LteScRSRQ_LT;
//		tsam.LteScEarfcn_LT = tTemp.LteScEarfcn_LT;
//		tsam.LteScPci_LT = tTemp.LteScPci_LT;
		tsam.iAreaID = tTemp.areaId;
		tsam.iAreaType = tTemp.areaType;
		// mdt 置信度
		tsam.Confidence = tTemp.Confidence;
		// 高铁信息
		tsam.trainKey = tTemp.trainKey;
		tsam.sectionId = tTemp.sectionId;
		tsam.segmentId = tTemp.segmentId;

		// 20171030 add QCI stat
		tsam.qciData = new LteScPlrQciData(tTemp.tsc.LteScPlrULQci, tTemp.tsc.LteScPlrDLQci);
		
		calJamType(tsam);

		try
		{
			 resultOutputer.pushData(mrosample, ResultHelper.getPutLteSample(tsam));
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MroVillageDeal output event error", "MroVillageDeal output event error ", e);
		}

	}
	
	public void calJamType(DT_Sample_4G tsam)
	{
		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						tsam.sfcnJamCellCount++;
					}
					else
					{
						tsam.dfcnJamCellCount++;
					}
				}
			}
		}
	}
	
	public int pushData(int dataType, int lng, int lat, String value)
	{
		if (dataType == DataType_VillageGrid)
		{
			villageLongitude = lng;
			villageLatitude = lat;
		}
		else if(dataType == DataType_Mro)
		{
			if(lng != villageLongitude || lat != villageLatitude)
			{
				return -1;
			}
			String strs[] = value.toString().split(StaticConfig.DataSliper2, -1);
			int cityid = Integer.parseInt(strs[0]);
			int longitude = Integer.parseInt(strs[1]);
			int latitude = Integer.parseInt(strs[2]);
			SIGNAL_MR_All mroItem = new SIGNAL_MR_All();
			Integer i = 3;
			try
			{
				mroItem.FillData(new Object[] { strs, i });
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MroVillageDeal SIGNAL_MR_All.FillData error", "MroVillageDeal SIGNAL_MR_All.FillData error ", e);
				return -1;
			}

			if (mroItem.tsc.MmeUeS1apId <= 0 || mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "MroVillageDeal mro format err :  " + value);
				return -1;
			}
			
			mroItem.tsc.cityID = cityid;
			mroItem.tsc.longitude = longitude;
			mroItem.tsc.latitude = latitude;

			mroItemList.add(mroItem);
		}
		return 0;
	}
	
	@Override
	public int pushData(int dataType, String value) {
		// TODO Auto-generated method stub
		
		return 0;
	}

	@Override
	public void statData() {
		// TODO Auto-generated method stub
		outDealingData(mroItemList);
		mroItemList.clear();
	}

	@Override
	public void outData() {
		// TODO Auto-generated method stub
		
	}

}
