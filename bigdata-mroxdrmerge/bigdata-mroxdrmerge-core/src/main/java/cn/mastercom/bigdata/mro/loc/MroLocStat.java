package cn.mastercom.bigdata.mro.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.mastercom.MrLocationPredict;
import cn.mastercom.bigdata.conf.config.CellBuildInfo;
import cn.mastercom.bigdata.loc.area.AreaModel;
import cn.mastercom.bigdata.loc.area.AreaPointFunc;
import cn.mastercom.exception.ModelNotTrainedException;
import cn.mastercom.locating.mr.EciMrData;
import org.apache.log4j.Logger;

import cn.mastercom.bigdata.StructData.MroOrigDataMT;
import cn.mastercom.bigdata.StructData.NC_GSM;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.loc.hsr.LocFunc;
import cn.mastercom.bigdata.loc.hsr.PositionModel;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class MroLocStat
{
	private static List<NC_LTE> nclteList;
	private static HashMap<Integer, NC_LTE> freqLteMap;
	private static Map<String, NC_GSM> ncGsmMap;
	protected static Logger LOG = Logger.getLogger(MroLocStat.class);

	public static Object getValidData(Object srcData, Object tarData)
	{
		if (tarData instanceof Integer)
		{
			if ((Integer) tarData != 0 && (Integer) tarData != StaticConfig.Int_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		else if (tarData instanceof Long)
		{
			if ((Long) tarData != 0 && (Long) tarData != StaticConfig.Long_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		return srcData;
	}

	private static void statLteNbCell(MroOrigDataMT item, List<NC_LTE> nclteList, HashMap<Integer, NC_LTE> freqLteMap)
	{
		if (item.LteNcRSRP != StaticConfig.Int_Abnormal && item.LteNcEarfcn > 0 && item.LteNcPci > 0)
		{
			NC_LTE data = new NC_LTE();
			data.LteNcEarfcn = item.LteNcEarfcn;
			data.LteNcPci = item.LteNcPci;
			data.LteNcRSRP = item.LteNcRSRP;
			data.LteNcRSRQ = item.LteNcRSRQ;
			data.LteNcEnodeb = item.LteNcEnodeb;//只有内蒙才有
			data.LteNcCid = item.LteNcCid;
			int freqType = Func.getFreqType(item.LteNcEarfcn);
			if (freqType == Func.YYS_YiDong)
			{
				nclteList.add(data);
			}
			else
			{
				if (!freqLteMap.containsKey(data.LteNcEarfcn)) {
					freqLteMap.put(data.LteNcEarfcn, data);
				}
				else if (freqLteMap.get(data.LteNcEarfcn).LteNcRSRP < data.LteNcRSRP)
				{
					freqLteMap.get(data.LteNcEarfcn).LteNcPci = data.LteNcPci;
					freqLteMap.get(data.LteNcEarfcn).LteNcRSRP = data.LteNcRSRP;
					freqLteMap.get(data.LteNcEarfcn).LteNcRSRQ = data.LteNcRSRQ;
					freqLteMap.get(data.LteNcEarfcn).LteNcEnodeb = data.LteNcEnodeb;
					freqLteMap.get(data.LteNcEarfcn).LteNcCid = data.LteNcCid;
				}
			}
		}
	}

	private static void statGsmNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item)
	{
		if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBcc > 0)
		{
			String key = item.GsmNcellBcch + "_" + item.GsmNcellBcc;

			NC_GSM data = ncGsmMap.get(key);
			if (data == null)
			{
				data = new NC_GSM();
				data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				data.GsmNcellBsic = item.GsmNcellNcc * 8 + item.GsmNcellBcc;
				data.GsmNcellBcch = item.GsmNcellBcch;
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
				{
					data.GsmNcellLac = item.GsmNcellLac;
					data.GsmNcellCi = item.GsmNcellCi;
				}

				ncGsmMap.put(key, data);
			}
			else
			{
				if (item.GsmNcellCarrierRSSI > data.GsmNcellCarrierRSSI)
				{
					data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				}
			}

		}
	}

//	private static void statTdsNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item)
//	{
//		if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0 && item.TdsCellParameterId > 0)
//		{
//			String key = item.TdsNcellUarfcn + "_" + item.TdsCellParameterId;
//
//			NC_TDS data = ncTdsMap.get(key);
//			if (data == null)
//			{
//				data = new NC_TDS();
//				data.TdsPccpchRSCP = item.TdsPccpchRSCP;
//				data.TdsNcellUarfcn = (short) item.TdsNcellUarfcn;
//				data.TdsCellParameterId = (short) item.TdsCellParameterId;
//
//				ncTdsMap.put(key, data);
//			}
//			else
//			{
//				if (item.TdsPccpchRSCP > data.TdsPccpchRSCP)
//				{
//					data.TdsPccpchRSCP = item.TdsPccpchRSCP;
//				}
//			}
//
//		}
//	}

	public static int getValidValueInt(int srcValue, int targValue)
	{
		if (targValue != StaticConfig.Int_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}
	
	public static double getValidValueDouble(double srcValue, double targValue)
	{
		if (targValue != StaticConfig.Double_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public static long getValidValueLong(long srcValue, long targValue)
	{
		if (targValue != StaticConfig.Long_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public static String getValidValueString(String srcValue, String targValue)
	{
		if (!targValue.equals(""))
		{
			return targValue;
		}
		return srcValue;
	}

	public static void groupMro(HashMap<String, ArrayList<MroOrigDataMT>> map, String value, DataAdapterReader dataAdapterReader, ParseItem parseItem)
	{
		try
		{
			StringBuffer bfKey = new StringBuffer();
			// long eci = 0;
			bfKey.setLength(0);
			String[] valstrs = value.split(parseItem.getSplitMark(), -1);
			dataAdapterReader.readData(valstrs);
			MroOrigDataMT item = new MroOrigDataMT();
			if (!item.FillData(dataAdapterReader))
			{
				return;
			}
			// 这包数据已经是同一个eci了
			bfKey.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
			bfKey.append("_");
			bfKey.append(dataAdapterReader.GetDateValue("beginTime", null));
			bfKey.append("_");
			bfKey.append(dataAdapterReader.GetStrValue("EventType", ""));
			String key = bfKey.toString();
			ArrayList<MroOrigDataMT> mroList = map.get(key);
			if (mroList == null)
			{
				mroList = new ArrayList<MroOrigDataMT>();
				map.put(key, mroList);
			}
			mroList.add(item);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			LOGHelper.GetLogger().writeLog(LogType.error, "groupMro error","groupMro error", e);
		}
	}

	public static void ottFix(SIGNAL_MR_All mroItem, XdrLabelMng xdrLableMng, LteCellInfo cellInfo)
	{
		if (mroItem == null || mroItem.tsc == null || mroItem.tsc.MmeUeS1apId <= 0 || mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
			return;
		// 附上地市id
		mroItem.tsc.cityID = cellInfo.cityid;
		//邻区回填ECI
		for (NC_LTE nc_LTE : mroItem.tlte)
		{//目前只有内蒙回填邻区enobid和ci，eci以此为准，如果没有enobid和ci，就通过工参匹配邻区ECI
			if (nc_LTE.LteNcEnodeb > 0 && nc_LTE.LteNcCid > 0)
			{
				nc_LTE.LteNcEci = nc_LTE.LteNcEnodeb * 256L + nc_LTE.LteNcCid;
			}
			else
			{
				LteCellInfo cell = CellConfig.GetInstance().getNearestCell(cellInfo.cityid, nc_LTE.LteNcEarfcn, nc_LTE.LteNcPci);
				if (cell != null)
				{
					nc_LTE.LteNcEci = cell.eci;
				}
				else
				{
					nc_LTE.LteNcEci = StaticConfig.Long_Abnormal;
				}
			}
		}
		xdrLableMng.dealMroData(mroItem);// ott定位
	}

	/**
	 * 没有format的mro解码定位
	 */
	public static void srcMroOttFixed(List<SIGNAL_MR_All> allMdtItemList, SIGNAL_MR_All mroItem, XdrLabelMng xdrLableMng, LteCellInfo cellInfo)
	{
		ottFix(mroItem, xdrLableMng, cellInfo);
		allMdtItemList.add(mroItem);
	}

	/**
	 * 指纹库算法3定位
	 */
	public static void UserLoc3Fixed(cn.mastercom.bigdata.locuser_v3.UserLocer userLocer, List<SIGNAL_MR_All> allMroItemList, ResultOutputer resultOutputer)
	{
		if (userLocer != null)
		{
			try
			{
				userLocer.DoWork(1, allMroItemList, resultOutputer);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"userLocer3.DoWork error", "userLocer3.DoWork error ", e);
			}
		}
	}

	public static SIGNAL_MR_All collectData(List<MroOrigDataMT> values)
	{
		nclteList = new ArrayList<>();
		ncGsmMap = new HashMap<String, NC_GSM>();
		freqLteMap = new HashMap<>();
		SIGNAL_MR_All mrResult = new SIGNAL_MR_All();
		for (MroOrigDataMT item : values)
		{
			try
			{
				mrResult.tsc.beginTime = (int) (item.beginTime.getTime() / 1000L);
				mrResult.tsc.beginTimems = (int) (item.beginTime.getTime() % 1000L);
			}
			catch (Exception e)
			{
				mrResult.tsc.beginTime = 0;
				mrResult.tsc.beginTimems = 0;
				continue;
			}

			//爱立信mr数据带经纬度
			if(item.longitude > 0){
				mrResult.tsc.longitude = getValidValueInt(mrResult.tsc.longitude, item.longitude);
				mrResult.tsc.latitude = getValidValueInt(mrResult.tsc.latitude, item.latitude);
				mrResult.locType = "mdt";
				mrResult.locSource = StaticConfig.LOCTYPE_HIGH;
			}
			mrResult.tsc.IMSI = 0;
			mrResult.tsc.TAC = 0;
			mrResult.tsc.ENBId = getValidValueInt(mrResult.tsc.ENBId, item.ENBId);
			mrResult.tsc.UserLabel = getValidValueString(mrResult.tsc.UserLabel, item.UserLabel);
			mrResult.tsc.Earfcn = getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);
			mrResult.tsc.MmeCode = getValidValueInt(mrResult.tsc.MmeCode, item.MmeCode);
			mrResult.tsc.MmeGroupId = getValidValueInt(mrResult.tsc.MmeGroupId, item.MmeGroupId);
			mrResult.tsc.MmeUeS1apId = getValidValueLong(mrResult.tsc.MmeUeS1apId, item.MmeUeS1apId);
			mrResult.tsc.Weight = getValidValueInt(mrResult.tsc.Weight, item.Weight);
			mrResult.tsc.EventType = getValidValueString(mrResult.tsc.EventType, item.EventType);
			mrResult.tsc.LteScRSRP = getValidValueInt(mrResult.tsc.LteScRSRP, item.LteScRSRP);
			mrResult.tsc.LteScRSRQ = getValidValueInt(mrResult.tsc.LteScRSRQ, item.LteScRSRQ);
			mrResult.tsc.LteScEarfcn = getValidValueInt(mrResult.tsc.LteScEarfcn, item.LteScEarfcn);
			mrResult.tsc.LteScPci = getValidValueInt(mrResult.tsc.LteScPci, item.LteScPci);
			mrResult.tsc.LteScBSR = getValidValueInt(mrResult.tsc.LteScBSR, item.LteScBSR);
			mrResult.tsc.LteScRTTD = getValidValueInt(mrResult.tsc.LteScRTTD, item.LteScRTTD);
			mrResult.tsc.LteScTadv = getValidValueInt(mrResult.tsc.LteScTadv, item.LteScTadv);
			mrResult.tsc.LteScAOA = getValidValueInt(mrResult.tsc.LteScAOA, item.LteScAOA);
			mrResult.tsc.LteScPHR = getValidValueInt(mrResult.tsc.LteScPHR, item.LteScPHR);
			mrResult.tsc.LteScSinrUL = getValidValueInt(mrResult.tsc.LteScSinrUL, item.LteScSinrUL);
			mrResult.tsc.LteScRIP = getValidValueDouble(mrResult.tsc.LteScRIP, item.LteScRIP);

			for (int i = 0; i < mrResult.tsc.LteScPlrULQci.length; ++i)
			{
				mrResult.tsc.LteScPlrULQci[i] = getValidValueDouble(mrResult.tsc.LteScPlrULQci[i], item.LteScPlrULQci[i]);
			}

			for (int i = 0; i < mrResult.tsc.LteScPlrDLQci.length; ++i)
			{
				mrResult.tsc.LteScPlrDLQci[i] = getValidValueDouble(mrResult.tsc.LteScPlrDLQci[i], item.LteScPlrDLQci[i]);
			}

			mrResult.tsc.LteScRI1 = getValidValueInt(mrResult.tsc.LteScRI1, item.LteScRI1);
			mrResult.tsc.LteScRI2 = getValidValueInt(mrResult.tsc.LteScRI2, item.LteScRI2);
			mrResult.tsc.LteScRI4 = getValidValueInt(mrResult.tsc.LteScRI4, item.LteScRI4);
			mrResult.tsc.LteScRI8 = getValidValueInt(mrResult.tsc.LteScRI8, item.LteScRI8);
			mrResult.tsc.LteScPUSCHPRBNum = getValidValueInt(mrResult.tsc.LteScPUSCHPRBNum, item.LteScPUSCHPRBNum);
			mrResult.tsc.LteScPDSCHPRBNum = getValidValueInt(mrResult.tsc.LteScPDSCHPRBNum, item.LteScPDSCHPRBNum);
			mrResult.tsc.LteSceNBRxTxTimeDiff = getValidValueInt(mrResult.tsc.LteSceNBRxTxTimeDiff, item.LteSceNBRxTxTimeDiff);

			mrResult.tsc.Eci = item.ENBId * 256L + item.CellId;
			mrResult.tsc.CellId = mrResult.tsc.Eci;

			if (mrResult.tsc.MmeUeS1apId <= 0 || mrResult.tsc.Eci <= 0)
			{
				return null;
			}

			statLteNbCell(item, nclteList, freqLteMap);
			statGsmNbCell(mrResult, item);
		}
		
		Collections.sort(nclteList, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP - o1.LteNcRSRP;
			}
		});
		
		// NC LTE
		int ydCount = 0;
		Iterator<NC_LTE> it = nclteList.iterator();
		//同频邻区放最前，最多取3组同频，rsrp逆序
		while(it.hasNext())
		{
			NC_LTE nc_LTE = it.next();
			if (nc_LTE.LteNcEarfcn == mrResult.tsc.LteScEarfcn) {
				if (ydCount < 3) {
					mrResult.tlte[ydCount] = nc_LTE;
					ydCount++;
				}
				it.remove();
			}
		}
		//剩余按rsrp大小取 （同频只有两组，则非同频按rsrp逆序取4组）
		for (NC_LTE nc_LTE :nclteList)
		{
			if (ydCount < mrResult.tlte.length) {
				mrResult.tlte[ydCount] = nc_LTE;
				ydCount++;
			}
		}
		mrResult.nccount[0] = (byte) ydCount;
		nclteList.clear();

		//freq
		List<Map.Entry<Integer, NC_LTE>> freqLteList = new ArrayList<Map.Entry<Integer, NC_LTE>>(freqLteMap.entrySet());
		Collections.sort(freqLteList, new Comparator<Map.Entry<Integer, NC_LTE>>()
		{
			public int compare(Map.Entry<Integer, NC_LTE> o1, Map.Entry<Integer, NC_LTE> o2)
			{
				return o2.getValue().LteNcRSRP - o1.getValue().LteNcRSRP;
			}
		});

		int dxCount = 0;
		int ltCount = 0;
		for (int i = 0; i < freqLteList.size(); ++i)
		{
			NC_LTE ncItem = freqLteList.get(i).getValue();
			int type = Func.getFreqType(ncItem.LteNcEarfcn);
			if (type == Func.YYS_LianTong)
			{
				if (ltCount < mrResult.lt_freq.length)
				{
					mrResult.lt_freq[ltCount] = ncItem;
					ltCount++;
				}
			}
			else if (type == Func.YYS_DianXin)
			{
				if (dxCount < mrResult.dx_freq.length)
				{
					mrResult.dx_freq[dxCount] = ncItem;
					dxCount++;
				}
			}
		}
		freqLteList.clear();
		freqLteMap.clear();

		// NC GSM
		// GSM只保留前1个邻区
		List<NC_GSM> ncGsmList = new ArrayList<NC_GSM>(ncGsmMap.values());
		ncGsmMap.clear();
		Collections.sort(ncGsmList, new Comparator<NC_GSM>()
		{
			public int compare(NC_GSM o1, NC_GSM o2)
			{
				return o2.GsmNcellCarrierRSSI - o1.GsmNcellCarrierRSSI;
			}
		});

		int count = mrResult.tgsm.length < ncGsmList.size() ? mrResult.tgsm.length : ncGsmList.size();
//			count = count > 1 ? 1 : count;
		mrResult.nccount[1] = (byte) count;

		for (int i = 0; i < count; ++i)
		{
			mrResult.tgsm[i] = ncGsmList.get(i);
		}
		ncGsmList.clear();
		ncGsmMap.clear();
		return mrResult;
	}

	/**
	 * 高铁定位
	 * 
	 * @param hsrLocFunc
	 * @param allMroItemList
	 */
	public static void HSRLocFixed(LocFunc hsrLocFunc, List<SIGNAL_MR_All> allMroItemList)
	{
		for (SIGNAL_MR_All mrAll : allMroItemList)
		{
			PositionModel position = hsrLocFunc.FindPosition(mrAll.tsc.IMSI, mrAll.tsc.beginTime, mrAll.tsc.Eci);
			if (position != null)
			{

				mrAll.locType = "hsr"; // 在新表统计的时候根据这个时段判断是否高铁定位的
				mrAll.testType = StaticConfig.TestType_HiRail;
				mrAll.locSource = 0; //将locSource置为非高中低，以避免吐出到非高铁的表
				mrAll.tsc.longitude = (int) (position.lng * 10000000);// double
																		// to
																		// int
				mrAll.tsc.latitude = (int) (position.lat * 10000000);
				mrAll.trainKey = position.trainKey;
				mrAll.sectionId = position.sectionid;
				mrAll.segmentId = position.segid;
			}
		}
	}

	public static void HSRAreaLocFixed(AreaPointFunc areaPointFunc, List<SIGNAL_MR_All> allMroItemList){
		for (SIGNAL_MR_All mrAll : allMroItemList)
		{
			AreaModel areaModel = areaPointFunc.findAreaPosition(mrAll.tsc.IMSI, mrAll.tsc.beginTime, null);
			if (areaModel != null)
			{

				mrAll.locType = "hsr"; // 在新表统计的时候根据这个时段判断是否高铁定位的
				mrAll.testType = StaticConfig.TestType_HiRail;
				mrAll.locSource = 0; //将locSource置为非高中低，以避免吐出到非高铁的表
				if(areaModel.longitude > 0 && mrAll.tsc.longitude <= 0){
					mrAll.tsc.longitude = (int) (areaModel.longitude * 10000000);// double
					// to
					// int
					mrAll.tsc.latitude = (int) (areaModel.latitude * 10000000);
				}
				mrAll.areaId =  areaModel.areaID;
				mrAll.areaType = areaModel.areaType;
			}
		}
	}

	public static void fillSpecialResidentData(SIGNAL_MR_All mrAll, CellBuildInfo cellBuildInfo, MrLocationPredict mrLocationPredict) {
		if (null == mrLocationPredict) {
			return;
		}
		mrAll.ibuildingID = predictBuildingId(mrItemToEciMrData(mrAll), mrLocationPredict);
		if (mrAll.ibuildingID <= 0) {
		    return;
        }
		String[] longitudeAndLatitude = acquireCellBuildingLngLat(cellBuildInfo, mrAll.ibuildingID);
		mrAll.tsc.longitude = Integer.parseInt(longitudeAndLatitude[0]);
		mrAll.tsc.latitude = Integer.parseInt(longitudeAndLatitude[1]);
		mrAll.locType = "bfp";
		mrAll.locSource = StaticConfig.LOCTYPE_LOW;
		mrAll.samState = StaticConfig.ACTTYPE_IN;
		mrAll.testType = StaticConfig.TestType_CQT;
		mrAll.label = "static";
	}

	public static EciMrData mrItemToEciMrData(SIGNAL_MR_All mrAll) {
		return new EciMrData("" + mrAll.tsc.IMSI, (int) mrAll.tsc.Eci,
				mrAll.tsc.LteScRSRP, mrAll.tsc.LteScRSRQ, mrAll.tsc.LteScBSR, mrAll.tsc.LteScTadv,
				mrAll.tsc.LteScAOA, mrAll.tsc.LteScPHR, mrAll.tsc.LteScSinrUL, mrAll.nccount[0],
				mrAll.tlte[0].LteNcRSRP, mrAll.tlte[0].LteNcRSRQ, mrAll.tlte[0].LteNcEarfcn, mrAll.tlte[0].LteNcPci,
				mrAll.tlte[1].LteNcRSRP, mrAll.tlte[1].LteNcRSRQ, mrAll.tlte[1].LteNcEarfcn, mrAll.tlte[1].LteNcPci,
				mrAll.tlte[2].LteNcRSRP, mrAll.tlte[2].LteNcRSRQ, mrAll.tlte[2].LteNcEarfcn, mrAll.tlte[2].LteNcPci,
				mrAll.tlte[3].LteNcRSRP, mrAll.tlte[3].LteNcRSRQ, mrAll.tlte[3].LteNcEarfcn, mrAll.tlte[3].LteNcPci,
				mrAll.tlte[4].LteNcRSRP, mrAll.tlte[4].LteNcRSRQ, mrAll.tlte[4].LteNcEarfcn, mrAll.tlte[4].LteNcPci,
				mrAll.tlte[5].LteNcRSRP, mrAll.tlte[5].LteNcRSRQ, mrAll.tlte[5].LteNcEarfcn, mrAll.tlte[5].LteNcPci);
	}

	public static int predictBuildingId(EciMrData eciMrData, MrLocationPredict mrLocationPredict) {
		return mrLocationPredict.predict(eciMrData);
	}

	private static String[] acquireCellBuildingLngLat(CellBuildInfo cellBuildInfo, int ibuildingId) {
		String centerLngLat = cellBuildInfo.getCenterLngLat(ibuildingId);
		if (null == centerLngLat) {
			return new String[]{String.valueOf(StaticConfig.Int_Abnormal), String.valueOf(StaticConfig.Int_Abnormal)};
		}
		return centerLngLat.split("_");
	}
}
