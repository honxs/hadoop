package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import scala.Tuple2;
import cn.mastercom.bigdata.StructData.*;

public class MapByMroFormat_spliterFile implements org.apache.spark.api.java.function.Function<Tuple2<String, Iterable<String>>, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Tuple2<String, Iterable<String>> tuple) throws Exception
	{
		LinkedList<String> linkedList = new LinkedList<String>();

		HashMap<String, NC_LTE> ncLteMap = new HashMap<String, NC_LTE>();
		HashMap<String, NC_GSM> ncGsmMap = new HashMap<String, NC_GSM>();
		HashMap<String, NC_TDS> ncTdsMap = new HashMap<String, NC_TDS>();

		SIGNAL_MR_All mrResult = new SIGNAL_MR_All();
		mrResult.Clear();
		String[] strs = null;
		for (String value : tuple._2)
		{
			strs = value.toString().split(",|\t");

			try
			{
				mrResult.tsc.beginTime = (int) (getTime(strs[0]) / 1000);
				mrResult.tsc.beginTimems = (int) (getTime(strs[0]) % 1000);
			}
			catch (Exception e)
			{
				mrResult.tsc.beginTime = 0;
				mrResult.tsc.beginTimems = 0;
				continue;
			}

			mrResult.tsc.Weight = Integer.parseInt(strs[9]);
			mrResult.tsc.IMSI = 0;
			mrResult.tsc.TAC = 0;
			mrResult.tsc.ENBId = Integer.parseInt(strs[1]);
			mrResult.tsc.Earfcn = Integer.parseInt(strs[4]);
			mrResult.tsc.MmeUeS1apId = Long.parseLong(strs[8]);
			mrResult.tsc.EventType = strs[10];
			mrResult.tsc.LteScRSRP = Integer.parseInt(strs[11]);
			mrResult.tsc.LteScRSRQ = Integer.parseInt(strs[13]);
			mrResult.tsc.LteScEarfcn = Integer.parseInt(strs[15]);
			mrResult.tsc.LteScPci = Integer.parseInt(strs[16]);
			mrResult.tsc.LteScBSR = Integer.parseInt(strs[26]);
			mrResult.tsc.LteScRTTD = Integer.parseInt(strs[27]);
			mrResult.tsc.LteScTadv = Integer.parseInt(strs[28]);
			mrResult.tsc.LteScAOA = Integer.parseInt(strs[29]);
			mrResult.tsc.LteScPHR = Integer.parseInt(strs[30]);
			mrResult.tsc.LteScSinrUL = Integer.parseInt(strs[32]);
			mrResult.tsc.LteScRIP = Integer.parseInt(strs[31]);

			int index = 33;
			for (int i = 0; i < mrResult.tsc.LteScPlrULQci.length; ++i)
			{
				mrResult.tsc.LteScPlrULQci[i] = Double.parseDouble(strs[index++]);
			}

			for (int i = 0; i < mrResult.tsc.LteScPlrDLQci.length; ++i)
			{
				mrResult.tsc.LteScPlrDLQci[i] = Double.parseDouble(strs[index++]);
			}

			mrResult.tsc.LteScRI1 = Integer.parseInt(strs[index++]);
			mrResult.tsc.LteScRI2 = Integer.parseInt(strs[index++]);
			mrResult.tsc.LteScRI4 = Integer.parseInt(strs[index++]);
			mrResult.tsc.LteScRI8 = Integer.parseInt(strs[index++]);
			mrResult.tsc.LteScPUSCHPRBNum = Integer.parseInt(strs[index++]);
			mrResult.tsc.LteScPDSCHPRBNum = Integer.parseInt(strs[index++]);
			mrResult.tsc.imeiTac = Integer.parseInt(strs[index++]);
			mrResult.tsc.Eci = Integer.parseInt(strs[1]) * 256 + Integer.parseInt(strs[3]);
			mrResult.tsc.CellId = mrResult.tsc.Eci;

			NcInfo nc = new NcInfo(Integer.parseInt(strs[17]), Integer.parseInt(strs[18]), Integer.parseInt(strs[12]), Integer.parseInt(strs[14]), Integer.parseInt(strs[19]), Integer.parseInt(strs[22]), Integer.parseInt(strs[20]), Integer.parseInt(strs[23]), Integer.parseInt(strs[24]), Integer.parseInt(strs[25]));

			statLteNbCell(mrResult, nc, ncLteMap);
			statGsmNbCell(mrResult, nc, ncGsmMap);
			statTdsNbCell(mrResult, nc, ncTdsMap);
		}

		if (mrResult.tsc.MmeUeS1apId > 0)
		{
			OutputOneMr(linkedList, ncLteMap, ncGsmMap, ncTdsMap, mrResult);
		}
		return mrResult.GetDataEx();
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

	private void statLteNbCell(SIGNAL_MR_All mrResult, NcInfo item, HashMap<String, NC_LTE> ncLteMap)
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

	private void statGsmNbCell(SIGNAL_MR_All mrResult, NcInfo item, HashMap<String, NC_GSM> ncGsmMap)
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

	private void statTdsNbCell(SIGNAL_MR_All mrResult, NcInfo item, HashMap<String, NC_TDS> ncTdsMap)
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

	private long getTime(String time)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		try
		{
			return sdf.parse(time).getTime();
		}
		catch (ParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0L;

	}

	class NcInfo
	{
		int LteNcEarfcn;
		int LteNcPci;
		int LteNcRSRP;
		int LteNcRSRQ;
		int GsmNcellCarrierRSSI;
		int GsmNcellBcc;
		int GsmNcellBcch;
		int TdsPccpchRSCP;
		int TdsNcellUarfcn;
		int TdsCellParameterId;

		public NcInfo(int earfcn, int pci, int rsrp, int rsrq, int rssi, int bcc, int bcch, int rscp, int uarfcn, int parameterId)
		{
			LteNcEarfcn = earfcn;
			LteNcPci = pci;
			LteNcRSRP = rsrp;
			LteNcRSRQ = rsrq;
			GsmNcellCarrierRSSI = rssi;
			GsmNcellBcc = bcc;
			GsmNcellBcch = bcch;
			TdsPccpchRSCP = rscp;
			TdsNcellUarfcn = uarfcn;
			TdsCellParameterId = parameterId;
		}
	}

}
