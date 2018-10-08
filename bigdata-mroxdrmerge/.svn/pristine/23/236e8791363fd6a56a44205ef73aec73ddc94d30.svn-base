package cn.mastercom.bigdata.spark.mroxdrmerge;

/*
 * TODO:此类与Mroloc_new中map相同
 * 可合并部分代码，后续可改
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import cn.mastercom.bigdata.StructData.*;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;
import cn.mastercom.bigdata.util.Func;

public class MapByCellFunc_MrAll implements org.apache.spark.api.java.function.PairFlatMapFunction<Iterator<String>, String, String>
{
	private static final long serialVersionUID = 1L;
	private final static int TimeSpan = 3600;
	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader_MROSRC;
	private SIGNAL_MR_All mrResult;
	private HashMap<String, NC_LTE> ncLteMap;
	private HashMap<String, NC_GSM> ncGsmMap;
	private HashMap<String, NC_TDS> ncTdsMap;
	
	public MapByCellFunc_MrAll() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		ncLteMap = new HashMap<String, NC_LTE>();
		ncGsmMap = new HashMap<String, NC_GSM>();
		ncTdsMap = new HashMap<String, NC_TDS>();

		mrResult = new SIGNAL_MR_All();
		mrResult.Clear();
		dataAdapterReader_MROSRC = new DataAdapterReader(parseItem);
	}
	
	
	@Override
	public Iterable<Tuple2<String, String>> call(Iterator<String> str) throws Exception
	{
		ArrayList<Tuple2<String, String>> list = new ArrayList<>();
	
		while(str.hasNext())
		{
			
			String value = str.next();
			boolean fillResult = true;
			String[] strs = null;
			strs = value.toString().split(parseItem.getSplitMark(), -1);
			MroOrigDataMT item = new MroOrigDataMT();
			try
			{
				dataAdapterReader_MROSRC.readData(strs);
				fillResult = item.FillData(dataAdapterReader_MROSRC);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MroOrigDataMT error", "MroOrigDataMT error ", e);
			}
			if (!fillResult)
			{
//				 continue;
			}
			
			int Weight = getValidValueInt(mrResult.tsc.Weight, item.Weight);
			if (Weight == 1 && mrResult.tsc.MmeUeS1apId > 0)
			{
				Tuple2<String, String> tp = new Tuple2<>(mrResult.tsc.Eci + "_" + mrResult.tsc.beginTime / TimeSpan * TimeSpan, OutputOneMr(ncLteMap, ncGsmMap, ncTdsMap, mrResult));
				list.add(tp);
			}
			try
			{
				mrResult.tsc.beginTime = (int) (item.beginTime.getTime() / 1000L);
				mrResult.tsc.beginTimems = (int) (item.beginTime.getTime() % 1000L);
			}
			catch (Exception e)
			{
				mrResult.tsc.beginTime = 0;
				mrResult.tsc.beginTimems = 0;
				// continue;
			}
			mrResult.tsc.Weight = Weight;
			mrResult.tsc.IMSI = 0;
			mrResult.tsc.TAC = 0;
			mrResult.tsc.ENBId = getValidValueInt(mrResult.tsc.ENBId, item.ENBId);
			mrResult.tsc.Earfcn = getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);

			if (Weight == 1 && mrResult.tsc.MmeUeS1apId <= 0)
			{
				mrResult.tsc.MmeUeS1apId = getValidValueLong(mrResult.tsc.MmeUeS1apId, item.MmeUeS1apId);
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

				mrResult.tsc.Eci = item.ENBId * 256 + item.CellId;
				mrResult.tsc.CellId = mrResult.tsc.Eci;
			}
			statLteNbCell(mrResult, item, ncLteMap);
			statGsmNbCell(mrResult, item, ncGsmMap);
			statTdsNbCell(mrResult, item, ncTdsMap);
		}

		if (mrResult.tsc.MmeUeS1apId > 0)
		{
//			list.add(OutputOneMr(ncLteMap, ncGsmMap, ncTdsMap, mrResult));//最后一条MRALL
			Tuple2<String, String> tp = new Tuple2<>(mrResult.tsc.Eci + "_" + mrResult.tsc.beginTime / TimeSpan * TimeSpan, OutputOneMr(ncLteMap, ncGsmMap, ncTdsMap, mrResult));
			list.add(tp);
		}
		return list;
	}
	
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
	
	private String OutputOneMr(HashMap<String, NC_LTE> ncLteMap, HashMap<String, NC_GSM> ncGsmMap, HashMap<String, NC_TDS> ncTdsMap,
			SIGNAL_MR_All mrResult)
	{
		// NC LTE
		List<Map.Entry<String, NC_LTE>> ncLteList = new ArrayList<Map.Entry<String, NC_LTE>>(ncLteMap.entrySet());
		Collections.sort(ncLteList, new Comparator<Map.Entry<String, NC_LTE>>()
		{
			public int compare(Map.Entry<String, NC_LTE> o1, Map.Entry<String, NC_LTE> o2)
			{
				return o2.getValue().LteNcRSRP - o1.getValue().LteNcRSRP;
			}
		});

		int cmccLteCount = 0;
		int dxCount = 0;
		int ltCount = 0;
//		int lteCount_Freq = 0;

//		NC_LTE nclte_lt = null;
//		NC_LTE nclte_dx = null;

		for (int i = 0; i < ncLteList.size(); ++i)
		{
			NC_LTE ncItem = ncLteList.get(i).getValue();

			int type = Func.getFreqType(ncItem.LteNcEarfcn);
			if (type == Func.YYS_YiDong)
			{
				if (cmccLteCount < mrResult.tlte.length)
				{
					mrResult.tlte[cmccLteCount] = ncItem;
					cmccLteCount++;
				}
			}
			else if (type == Func.YYS_LianTong)
			{
				if (ltCount < mrResult.lt_freq.length)
				{
					boolean replace = false;
					for (int lt = 0; lt < ltCount; lt++)
					{
						//频点相同的话，取rsrp值更大的
						if (ncItem.LteNcEarfcn == mrResult.lt_freq[lt].LteNcEarfcn)
						{
							replace = true;
							break;
						}
					}
					if (!replace)
					{
						mrResult.lt_freq[ltCount] = ncItem;
						ltCount++;
					}
				}
			}
			else if (type == Func.YYS_DianXin)
			{
				if (dxCount < mrResult.dx_freq.length)
				{
					boolean replace = false;
					for (int dx = 0; dx < dxCount; dx++)
					{
						//频点相同的话，取rsrp值更大的
						if (ncItem.LteNcEarfcn == mrResult.dx_freq[dx].LteNcEarfcn)
						{
							replace = true;
							break;
						}
					}
					if (!replace)
					{
						mrResult.dx_freq[dxCount] = ncItem;
						dxCount++;
					}
				}
			}
		}

		mrResult.nccount[0] = (byte) cmccLteCount;
//		mrResult.nccount[2] = (byte) (ltCount+dxCount);

		// NC TDS
		// TD只保留前2个邻区
		List<Map.Entry<String, NC_TDS>> ncTdsList = new ArrayList<Map.Entry<String, NC_TDS>>(ncTdsMap.entrySet());
		Collections.sort(ncTdsList, new Comparator<Map.Entry<String, NC_TDS>>()
		{
			public int compare(Map.Entry<String, NC_TDS> o1, Map.Entry<String, NC_TDS> o2)
			{
				return o2.getValue().TdsPccpchRSCP - o1.getValue().TdsPccpchRSCP;
			}
		});

//		int count = mrResult.ttds.length < ncTdsList.size() ? mrResult.ttds.length : ncTdsList.size();
//		count = count > 2 ? 2 : count;
//		mrResult.nccount[1] = (byte) count;
//
//		for (int i = 0; i < count; ++i)
//		{
//			mrResult.ttds[i] = ncTdsList.get(i).getValue();
//		}

		// NC GSM
		// GSM只保留前1个邻区
		List<Map.Entry<String, NC_GSM>> ncGsmList = new ArrayList<Map.Entry<String, NC_GSM>>(ncGsmMap.entrySet());
		Collections.sort(ncGsmList, new Comparator<Map.Entry<String, NC_GSM>>()
		{
			public int compare(Map.Entry<String, NC_GSM> o1, Map.Entry<String, NC_GSM> o2)
			{
				return o2.getValue().GsmNcellCarrierRSSI - o1.getValue().GsmNcellCarrierRSSI;
			}
		});

		int count = mrResult.tgsm.length < ncGsmList.size() ? mrResult.tgsm.length : ncGsmList.size();
		count = count > 1 ? 1 : count;
		mrResult.nccount[1] = (byte) count;

		for (int i = 0; i < count; ++i)
		{
			mrResult.tgsm[i] = ncGsmList.get(i).getValue();
		}

		String dataEx = mrResult.GetDataEx();
		mrResult.Clear();
		ncLteMap.clear();
		ncGsmMap.clear();
		ncTdsMap.clear();
		return dataEx;
//		return new Tuple2<>(mrResult.tsc.Eci+"_"+mrResult.tsc.beginTime / TimeSpan * TimeSpan, dataEx);
	}
	
	private static void statLteNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item, HashMap<String, NC_LTE> ncLteMap)
	{
		if (item.LteNcRSRP != StaticConfig.Int_Abnormal && item.LteNcEarfcn > 0 && item.LteNcPci > 0)
		{
			String key = item.LteNcEarfcn + "_" + item.LteNcPci;

			NC_LTE data = ncLteMap.get(key);
			if (data == null)
			{
				data = new NC_LTE();
				data.LteNcEarfcn = item.LteNcEarfcn;
				data.LteNcPci = item.LteNcPci;
				data.LteNcRSRP = item.LteNcRSRP;
				data.LteNcRSRQ = item.LteScRSRQ;

				ncLteMap.put(key, data);
			}
			else
			{
				if (item.LteNcRSRP > data.LteNcRSRP)
				{
					data.LteNcRSRP = item.LteNcRSRP;
					data.LteNcRSRQ = item.LteNcRSRQ;
				}
			}
		}
	}

	private static void statGsmNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item, HashMap<String, NC_GSM> ncGsmMap)
	{
		if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBcc > 0)
		{
			String key = item.GsmNcellBcch + "_" + item.GsmNcellBcc;

			NC_GSM data = ncGsmMap.get(key);
			if (data == null)
			{
				data = new NC_GSM();
				data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				data.GsmNcellBsic = item.GsmNcellBcc;
				data.GsmNcellBcch = item.GsmNcellBcch;

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

	private static void statTdsNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item, HashMap<String, NC_TDS> ncTdsMap)
	{
		if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0 && item.TdsCellParameterId > 0)
		{
			String key = item.TdsNcellUarfcn + "_" + item.TdsCellParameterId;

			NC_TDS data = ncTdsMap.get(key);
			if (data == null)
			{
				data = new NC_TDS();
				data.TdsPccpchRSCP = item.TdsPccpchRSCP;
				data.TdsNcellUarfcn = (short) item.TdsNcellUarfcn;
				data.TdsCellParameterId = (short) item.TdsCellParameterId;

				ncTdsMap.put(key, data);
			}
			else
			{
				if (item.TdsPccpchRSCP > data.TdsPccpchRSCP)
				{
					data.TdsPccpchRSCP = item.TdsPccpchRSCP;
				}
			}

		}
	}


}
