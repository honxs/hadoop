package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

import org.apache.spark.api.java.function.FlatMapFunction;

import cn.mastercom.bigdata.StructData.*;

public class MapPartitionFunc_MrFormat implements FlatMapFunction<Iterator<String>, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ParseItem parseItem;
	private int splitMax = -1;

	public MapPartitionFunc_MrFormat() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}

	}

	@Override
	public Iterable<String> call(Iterator<String> integerIterator) throws Exception
	{

		LinkedList<String> linkedList = new LinkedList<String>();

		HashMap<String, NC_LTE> ncLteMap = new HashMap<String, NC_LTE>();
		HashMap<String, NC_GSM> ncGsmMap = new HashMap<String, NC_GSM>();
		HashMap<String, NC_TDS> ncTdsMap = new HashMap<String, NC_TDS>();

		boolean fillResult = true;

		SIGNAL_MR_All mrResult = new SIGNAL_MR_All();
		mrResult.Clear();
		String value = "";
		String[] strs = null;
		DataAdapterReader dataAdapterReader_MROSRC = new DataAdapterReader(parseItem);
		while (integerIterator.hasNext())
		{
			value = integerIterator.next();
			strs = value.toString().split(parseItem.getSplitMark(), -1);

			MroOrigDataMT item = new MroOrigDataMT();

			try
			{
				dataAdapterReader_MROSRC.readData(strs);
				fillResult = item.FillData(dataAdapterReader_MROSRC);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MroOrigDataMT error1", "MroOrigDataMT error1 ", e);
				continue;
			}

			if (!fillResult)
			{
				continue;
			}

			int Weight = getValidValueInt(mrResult.tsc.Weight, item.Weight);
			if (Weight == 1 && mrResult.tsc.MmeUeS1apId > 0)
			{
				OutputOneMr(linkedList, ncLteMap, ncGsmMap, ncTdsMap, mrResult);
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
				continue;
			}

			mrResult.tsc.Weight = Weight;
			mrResult.tsc.IMSI = 0;
			mrResult.tsc.TAC = 0;
			mrResult.tsc.ENBId = getValidValueInt(mrResult.tsc.ENBId, item.ENBId);
			// mrResult.tsc.UserLabel =
			// getValidValueString(mrResult.tsc.UserLabel, item.UserLabel);
			mrResult.tsc.Earfcn = getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);
			// mrResult.tsc.MmeCode = getValidValueInt(mrResult.tsc.MmeCode,
			// item.MmeCode);
			// mrResult.tsc.MmeGroupId =
			// getValidValueInt(mrResult.tsc.MmeGroupId, item.MmeGroupId);
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
				mrResult.tsc.imeiTac = getValidValueInt(mrResult.tsc.imeiTac, 0);

				mrResult.tsc.Eci = item.ENBId * 256 + item.CellId;
				mrResult.tsc.CellId = mrResult.tsc.Eci;
			}

			statLteNbCell(mrResult, item, ncLteMap);
			statGsmNbCell(mrResult, item, ncGsmMap);
			statTdsNbCell(mrResult, item, ncTdsMap);
		}

		if (mrResult.tsc.MmeUeS1apId > 0)
		{
			OutputOneMr(linkedList, ncLteMap, ncGsmMap, ncTdsMap, mrResult);
		}
		return linkedList;
	}

	private void OutputOneMr(LinkedList<String> linkedList, HashMap<String, NC_LTE> ncLteMap, HashMap<String, NC_GSM> ncGsmMap, HashMap<String, NC_TDS> ncTdsMap, SIGNAL_MR_All mrResult)
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
		int lteCount_Freq = 0;

//		for (int i = 0; i < ncLteList.size(); ++i)
//		{
//			NC_LTE item1 = ncLteList.get(i).getValue();
//
//			if (mrResult.fillNclte_Freq(item1))
//			{
//				lteCount_Freq++;
//			}
//			else
//			{
//				if (cmccLteCount < mrResult.tlte.length)
//				{
//					mrResult.tlte[cmccLteCount] = item1;
//					cmccLteCount++;
//				}
//			}
//
//		}

		mrResult.nccount[0] = (byte) cmccLteCount;
//		mrResult.nccount[2] = (byte) (lteCount_Freq);

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

		linkedList.add(mrResult.GetDataEx());
		mrResult.Clear();
		ncLteMap.clear();
		ncGsmMap.clear();
		ncTdsMap.clear();
	}

	private void statLteNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item, HashMap<String, NC_LTE> ncLteMap)
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
				data.LteNcRSRQ = item.LteNcRSRQ;

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

	private void statGsmNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item, HashMap<String, NC_GSM> ncGsmMap)
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

	private void statTdsNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item, HashMap<String, NC_TDS> ncTdsMap)
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

	private int getValidValueInt(int srcValue, int targValue)
	{
		if (targValue != StaticConfig.Int_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}
	
	private double getValidValueDouble(double srcValue, double targValue)
	{
		if (targValue != StaticConfig.Double_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	private long getValidValueLong(long srcValue, long targValue)
	{
		if (targValue != StaticConfig.Long_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	private String getValidValueString(String srcValue, String targValue)
	{
		if (!targValue.equals(""))
		{
			return targValue;
		}
		return srcValue;
	}

}
