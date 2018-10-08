package cn.mastercom.bigdata.StructData;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import cn.mastercom.MrLocationPredict;
import cn.mastercom.bigdata.base.model.ExternalDO;
import cn.mastercom.bigdata.conf.config.CellBuildInfo;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.DataGeter;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.GisPos;
import cn.mastercom.bigdata.util.StringHelper;
import cn.mastercom.bigdata.xdr.loc.MrBuildCell;
import cn.mastercom.exception.ModelNotTrainedException;
import cn.mastercom.locating.mr.EciMrData;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;

public class SIGNAL_MR_All implements Serializable,ExternalDO
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final int NCCOUNT_ARRAY_LENGTH = 2;
	public static final int NCLTE_ARRAY_LENGTH = 6;
	public static final int NCGSM_ARRAY_LENGTH = 3;
//	public static final int TRIP_ARRAY_LENGTH = 10;
	public static final int DXFREQ_ARRAY_LENGTH = 3;
	public static final int LTFREQ_ARRAY_LENGTH = 3;
	
	public final short[] nccount = new short[NCCOUNT_ARRAY_LENGTH];
	public final NC_LTE[] tlte = new NC_LTE[NCLTE_ARRAY_LENGTH];
	public final NC_GSM[] tgsm = new NC_GSM[NCGSM_ARRAY_LENGTH];
//	public final SC_FRAME[] trip = new SC_FRAME[TRIP_ARRAY_LENGTH];
	public final NC_LTE[] dx_freq = new NC_LTE[DXFREQ_ARRAY_LENGTH];
	public final NC_LTE[] lt_freq = new NC_LTE[LTFREQ_ARRAY_LENGTH];

	// xdr 回填信息
	public SIGNAL_MR_SC tsc;
	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String locType = "";// 指纹库定位fp
	public int indoor;
	public String networktype = "";
	public String label = "";
	public int serviceType;
	public int subServiceType;
	public int moveDirect;
	public int ibuildingID = -1;
	public short iheight = -1;
	public int simuLongitude;
	public int simuLatitude;
	public String UETac = "";
	// 添加字段5分钟内小区切换信息
	public String eciSwitchList = "";
	public int locSource;// 位置来源high/mid/low
	public int samState;// int or out
	// add 场景统计标识
	public int areaId = -1;
	public int areaType = -1;

	public int Confidence;// mdt 置信度
	public int ConfidenceType;// 置信度类型

	// 20171129 add 高铁统计
	public int sectionId;
	public int segmentId;
	public long trainKey;
	public int position;
	public boolean isResidentUser; //常驻用户标签

	public SIGNAL_MR_All()
	{
		Clear();
	}

	public void Clear()
	{
		tsc = new SIGNAL_MR_SC();
		for (int i = 0; i < nccount.length; i++)
			nccount[i] = 0;

		for (int i = 0; i < 6; i++)
		{
			tlte[i] = new NC_LTE();
			tlte[i].Clear();
		}
		
//		for (int i = 0; i < trip.length; i++)
//		{
//			trip[i] = new SC_FRAME();
//			trip[i].Clear();
//		}
		
		for (int i = 0; i < 3; i++)
		{
			tgsm[i] = new NC_GSM();
			tgsm[i].Clear();
			dx_freq[i] = new NC_LTE();
			dx_freq[i].Clear();
			lt_freq[i] = new NC_LTE();
			lt_freq[i].Clear();
		}

		sectionId = StaticConfig.Natural_Abnormal;
		segmentId = StaticConfig.Natural_Abnormal;
		trainKey = StaticConfig.Natural_Abnormal;
		position = 0;
		testType = -1;
		location = -1;
		dist = -1;
		radius = -1;
		locType = "";
		indoor = -1;
		networktype = "";
		label = "";
		serviceType = StaticConfig.Int_Abnormal;
		subServiceType = StaticConfig.Int_Abnormal;
		moveDirect = StaticConfig.Natural_Abnormal;
		ibuildingID = StaticConfig.Natural_Abnormal;
		iheight = StaticConfig.Natural_Abnormal;
		simuLongitude = StaticConfig.Int_Abnormal;
		simuLatitude = StaticConfig.Int_Abnormal;
		UETac = StaticConfig.String_Abnormal;
		eciSwitchList = StaticConfig.String_Abnormal;
		locSource = 0;
		samState = StaticConfig.Natural_Abnormal;
		areaId = StaticConfig.Natural_Abnormal;
		areaType = StaticConfig.Natural_Abnormal;
		Confidence = StaticConfig.Natural_Abnormal;
		ConfidenceType = StaticConfig.Natural_Abnormal;
		isResidentUser = false;
	}

	public String GetData()
	{
		StringBuffer res = new StringBuffer();

		res.append(tsc.GetData());
		res.append(StaticConfig.DataSlipter);

		for (int data : nccount)
		{
			res.append(data);
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_LTE data : tlte)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

//		for (NC_TDS data : ttds)
//		{
//			res.append(data.GetData());
//			res.append(StaticConfig.DataSlipter);
//		}

		for (NC_GSM data : tgsm)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

//		for (SC_FRAME data : trip)
//		{
//			res.append(data.GetData());
//			res.append(StaticConfig.DataSlipter);
//		}
		
		for (NC_LTE data : dx_freq)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}
		
		for (NC_LTE data : lt_freq)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

//		res.append(LteScRSRP_DX);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScRSRQ_DX);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScEarfcn_DX);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScPci_DX);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScRSRP_LT);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScRSRQ_LT);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScEarfcn_LT);
//		res.append(StaticConfig.DataSlipter);
//		res.append(LteScPci_LT);
//		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
//		{
//			for (NC_LTE data : tlte)
//			{
//				res.append(StaticConfig.DataSlipter);
//				res.append(data.GetDataAll());
//			}
//			for (NC_GSM data : tgsmall)
//			{
//				res.append(StaticConfig.DataSlipter);
//				res.append(data.GetDataAll());
//			}
//		}

		return StringHelper.SideTrim(res.toString(), StaticConfig.DataSlipter);
	}

	private String tmStr;

	public String GetDataEx()
	{
		tmStr = GetData();
		tmStr = tmStr.replace("" + StaticConfig.Double_Abnormal, "");
		tmStr = tmStr.replace("" + StaticConfig.Int_Abnormal, "");
		tmStr = tmStr.replace("" + StaticConfig.Short_Abnormal, "");
		return tmStr;
	}

	public boolean FillData(Object[] args)
	{
		tsc.FillData(args);
		String values[] = (String[]) args[0];
		Integer i = (Integer) args[1];
		for (int ii = 0; ii < nccount.length; ii++)
			nccount[ii] = DataGeter.GetTinyInt(values[i + ii]);
		i += nccount.length;

		args[1] = i;
		for (int ii = 0; ii < tlte.length; ii++)
		{
			tlte[ii].FillData(args);
		}

		if(tsc.longitude > 0){//爱立信的数据有经纬度
			locSource = StaticConfig.LOCTYPE_MID;
			testType = StaticConfig.TestType_DT;
		}

//		for (int ii = 0; ii < ttds.length; ii++)
//			ttds[ii].FillData(args);

		for (int ii = 0; ii < tgsm.length; ii++)
			tgsm[ii].FillData(args);

//		for (int ii = 0; ii < trip.length; ii++)
//			trip[ii].FillData(args);
		
		for (int ii = 0; ii < dx_freq.length; ii++)
			dx_freq[ii].FillData(args);
		
		for (int ii = 0; ii < lt_freq.length; ii++)
			lt_freq[ii].FillData(args);
//		int pos = (Integer) args[1];
//		if (pos + 8 <= values.length)
//		{
//			LteScRSRP_DX = DataGeter.GetInt(values[pos++], 0);
//			LteScRSRQ_DX = DataGeter.GetInt(values[pos++], 0);
//			LteScEarfcn_DX = DataGeter.GetInt(values[pos++], 0);
//			LteScPci_DX = DataGeter.GetInt(values[pos++], 0);
//			LteScRSRP_LT = DataGeter.GetInt(values[pos++], 0);
//			LteScRSRQ_LT = DataGeter.GetInt(values[pos++], 0);
//			LteScEarfcn_LT = DataGeter.GetInt(values[pos++], 0);
//			LteScPci_LT = DataGeter.GetInt(values[pos++], 0);
			
//		}
//		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
//		{
//			args[1] = pos;
//			for (int ii = 0; ii < tlte.length; ii++)
//				tlte[ii].FillDataAll(args);
//			for (int ii = 0; ii < tgsmall.length; ii++)
//				tgsmall[ii].FillDataAll(args);
//		}

		return true;

	}

	public boolean FillIMMData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
//		tsc.beginTime = (int) (dataAdapterReader.GetDateValue("beginTime", null).getTime() / 1000);
		Date d_beginTime = dataAdapterReader.GetDateValue("beginTime", null);
		if(d_beginTime == null)
			throw new IllegalArgumentException("ERROR:数据缺少合法的时间字段。");
		tsc.beginTime = (int) (d_beginTime.getTime() / 1000L);
		tsc.beginTimems = (int) (d_beginTime.getTime() % 1000L);
		tsc.ENBId = dataAdapterReader.GetIntValue("ENBId", StaticConfig.Int_Abnormal);
		tsc.Eci = dataAdapterReader.GetIntValue("CellId", StaticConfig.Int_Abnormal);
		if(tsc.Eci <= 0)
			throw new IllegalArgumentException("ERROR:数据缺少合法的Eci字段。");
		tsc.Earfcn = dataAdapterReader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		tsc.IMSI = dataAdapterReader.GetLongValue("IMSI", StaticConfig.Int_Abnormal);
		tsc.MmeGroupId = dataAdapterReader.GetIntValue("MmeGroupId", StaticConfig.Int_Abnormal);
		tsc.MmeCode = dataAdapterReader.GetIntValue("MmeCode", StaticConfig.Int_Abnormal);
		tsc.MmeUeS1apId = dataAdapterReader.GetLongValue("MmeUeS1apId", StaticConfig.Int_Abnormal);
		tsc.LteScRSRP = rsrpFormat(dataAdapterReader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal));
		tsc.LteScRSRQ = rsrqFormat(dataAdapterReader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal));
		tsc.LteScEarfcn = dataAdapterReader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		tsc.LteScPci = dataAdapterReader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);
		
		// 置信度
		//对于高精度的mdt数据，可以认为在室外，否则有可能就是室内
		Confidence = dataAdapterReader.GetIntValue("Confidence", StaticConfig.Int_Abnormal);
		if (Confidence >= 0 && Confidence <= 50)
		{
			locSource = StaticConfig.LOCTYPE_LOW;
			testType = StaticConfig.TestType_CQT;
		}
		else if (Confidence > 50 && Confidence <= 100)
		{
			locSource = StaticConfig.LOCTYPE_HIGH;
			testType = StaticConfig.TestType_DT;
		}else { //20180331 暂时 对于没有填写该字段的一律当作高精度
			locSource = StaticConfig.LOCTYPE_HIGH;
			testType = StaticConfig.TestType_DT;
		}
		GisPos gisPos = new GisPos(dataAdapterReader.GetDoubleValue("longitude", 0), dataAdapterReader.GetDoubleValue("latitude", 0));
		tsc.longitude = (int) (gisPos.getWgLon() * 10000000D);
		tsc.latitude = (int) (gisPos.getWgLat() * 10000000D);
		tsc.EventType = "MDT_IMM";
		locType = "mdt";

		FillDataPciRsrp(dataAdapterReader);

		return true;
	}

	private void FillDataPciRsrp(DataAdapterReader dataAdapterReader) throws ParseException
	{
		List<NC_LTE> ncLteList = new ArrayList<>();
		for (int i = 1; i <= 6; i++)
		{
			int LteNcRSRP = rsrpFormat(dataAdapterReader.GetIntValue("LteNcRSRP" + i, StaticConfig.Int_Abnormal));
			int LteNcRSRQ = rsrqFormat(dataAdapterReader.GetIntValue("LteNcRSRQ" + i, StaticConfig.Int_Abnormal));
			int LteNcEarfcn = dataAdapterReader.GetIntValue("LteNcEarfcn" + i, StaticConfig.Int_Abnormal);
			int LteNcPci = dataAdapterReader.GetIntValue("LteNcPci" + i, StaticConfig.Int_Abnormal);
			NC_LTE nc_LTE = new NC_LTE(LteNcRSRP, LteNcRSRQ, LteNcEarfcn, LteNcPci);
			ncLteList.add(nc_LTE);
		}
		Collections.sort(ncLteList, new Comparator<NC_LTE>()
		{
			@Override
			public int compare(NC_LTE o1, NC_LTE o2)
			{
				return o2.LteNcRSRP - o1.LteNcRSRP;
			}
		});
		int cmccLteCount = 0;
		int dxCount = 0;
		int ltCount = 0;

		for (int i = 0; i < ncLteList.size(); ++i)
		{
			NC_LTE ncItem = ncLteList.get(i);

			int type = Func.getFreqType(ncItem.LteNcEarfcn);
			if (type == Func.YYS_YiDong)
			{
				if (cmccLteCount < tlte.length)
				{
					tlte[cmccLteCount] = ncItem;
					cmccLteCount++;
				}
			}
			else if (type == Func.YYS_LianTong)
			{
				if (ltCount < lt_freq.length)
				{
					boolean replace = false;
					for (int lt=0; lt<ltCount;lt++)
					{
						//频点相同的话，取rsrp值更大的
						if (ncItem.LteNcEarfcn == lt_freq[lt].LteNcEarfcn)
						{
							replace = true;
							break;
						}
					}
					if (!replace)
					{
						lt_freq[ltCount] = ncItem;
						ltCount++;
					}
				}
			}
			else if (type == Func.YYS_DianXin)
			{
				if (dxCount < dx_freq.length)
				{
					boolean replace = false;
					for (int dx=0; dx<dxCount;dx++)
					{
						//频点相同的话，取rsrp值更大的
						if (ncItem.LteNcEarfcn == dx_freq[dx].LteNcEarfcn)
						{
							replace = true;
							break;
						}
					}
					if (!replace)
					{
						dx_freq[dxCount] = ncItem;
						dxCount++;
					}
				}
			}
		}

//		// 添加联通数据
//		if (nclte_lt != null && fillNclte_Freq(nclte_lt))
//		{
//			lteCount_Freq++;
//		}
//		// 添加电信数据
//		if (nclte_dx != null && fillNclte_Freq(nclte_dx))
//		{
//			lteCount_Freq++;
//		}

		nccount[0] = (byte) cmccLteCount;
//		nccount[2] = (byte) (ltCount+dxCount);
	}

	public boolean FillLOGData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
//		tsc.beginTime = (int) (dataAdapterReader.GetDateValue("beginTime", null).getTime() / 1000);
		Date d_beginTime = dataAdapterReader.GetDateValue("beginTime", null);
		if(d_beginTime == null)
			throw new IllegalArgumentException("ERROR:数据缺少合法的时间字段。");
		tsc.beginTime = (int) (d_beginTime.getTime() / 1000L);
		tsc.beginTimems = (int) (d_beginTime.getTime() % 1000L);
		tsc.ENBId = dataAdapterReader.GetIntValue("ENBId", StaticConfig.Int_Abnormal);
		tsc.Eci = dataAdapterReader.GetIntValue("CellId", StaticConfig.Int_Abnormal);
		if(tsc.Eci <= 0)
			throw new IllegalArgumentException("ERROR:数据缺少合法的Eci字段。");
		tsc.IMSI = dataAdapterReader.GetLongValue("IMSI", StaticConfig.Int_Abnormal);
		tsc.MmeGroupId = dataAdapterReader.GetIntValue("MmeGroupId", StaticConfig.Int_Abnormal);
		tsc.MmeCode = dataAdapterReader.GetIntValue("MmeCode", StaticConfig.Int_Abnormal);
		tsc.MmeUeS1apId = dataAdapterReader.GetLongValue("MmeUeS1apId", StaticConfig.Int_Abnormal);
		tsc.LteScRSRP = rsrpFormat(dataAdapterReader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal));
		tsc.LteScRSRQ = rsrqFormat(dataAdapterReader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal));
		GisPos gisPos = new GisPos(dataAdapterReader.GetDoubleValue("longitude", 0), dataAdapterReader.GetDoubleValue("latitude", 0));

		tsc.longitude = (int) (gisPos.getWgLon() * 10000000D);
		tsc.latitude = (int) (gisPos.getWgLat() * 10000000D);
		tsc.EventType = "MDT_LOG";
		locType = "mdt";
		
		// 置信度
		Confidence = dataAdapterReader.GetIntValue("Confidence", StaticConfig.Int_Abnormal);
		if (Confidence >= 0 && Confidence <= 50)
		{
			locSource = StaticConfig.LOCTYPE_LOW;
			testType = StaticConfig.TestType_CQT;
		}
		else if (Confidence > 50 && Confidence <= 100)
		{
			locSource = StaticConfig.LOCTYPE_HIGH;
			testType = StaticConfig.TestType_DT;
		}else { //20180331 暂时 对于没有填写该字段的一律当作高精度
			locSource = StaticConfig.LOCTYPE_HIGH;
			testType = StaticConfig.TestType_DT;
		}

		FillDataPciRsrp(dataAdapterReader);
		return true;
	}

	public boolean FillData_UEMro(Object[] args)
	{
		String values[] = (String[]) args[0];
		int startPos = 0;// DataGeter.GetInt((String)args[1]);
		// tsc.cityID = DataGeter.GetInt(values[i++]);
		tsc.IMSI = DataGeter.GetLong(values[startPos + 5]);
		// tsc.MmeGroupId = DataGeter.GetInt(values[startPos+8]);
		// tsc.MmeCode = DataGeter.GetInt(values[startPos+9]);
		tsc.MmeUeS1apId = DataGeter.GetLong(values[startPos + 14]);
		tsc.ENBId = Integer.parseInt(values[startPos + 15]);
		tsc.CellId = DataGeter.GetLong(values[startPos + 16]);
		tsc.Eci = tsc.CellId;
		tsc.beginTime = (int) (DataGeter.GetLong(values[startPos + 17]) / 1000);
		tsc.beginTimems = (int) (DataGeter.GetLong(values[startPos + 17]) % 1000);
//		tsc.EventType = DataGeter.GetInt(values[startPos + 14]) + "";
		 tsc.LteScPHR = DataGeter.GetInt(values[startPos+19]);
		tsc.LteScSinrUL = DataGeter.GetInt(values[startPos + 21]);
		tsc.LteScTadv = DataGeter.GetInt(values[startPos + 22]);
		tsc.LteScAOA = DataGeter.GetInt(values[startPos + 23]);
		tsc.LteScEarfcn = DataGeter.GetInt(values[startPos + 24]);
		tsc.LteScRSRP = DataGeter.GetInt(values[startPos + 25]);
		tsc.LteScRSRQ = DataGeter.GetInt(values[startPos+26]);
		tsc.LteScPci = DataGeter.GetInt(values[startPos+28]);
		int nbcellnum = DataGeter.GetInt(values[startPos + 27]);
		
		
		/*List<NC_LTE> ncLteList = new ArrayList<>();
		HashMap<Integer, NC_LTE> freqLteMap = new HashMap<>();
		nbcellnum = nbcellnum > 6 ? 6 : nbcellnum;*/
		int pos = startPos + 29;
		NC_LTE data = null;
		List<NC_LTE> sameEarfcnList = new ArrayList<>();
		List<NC_LTE> nclteList = new ArrayList<>();
		for (int i = 0; i < nbcellnum; ++i)
		{
			data = new NC_LTE();
			data.LteNcPci = DataGeter.GetInt(values[pos++]);
			data.LteNcEarfcn = DataGeter.GetInt(values[pos++]);
			data.LteNcRSRP = DataGeter.GetInt(values[pos++]);
			data.LteNcRSRQ = DataGeter.GetInt(values[pos++]);
			
			if (data.LteNcEarfcn == tsc.LteScEarfcn){
				sameEarfcnList.add(data);
			}else {
				nclteList.add(data);
			}
		}
		
		//前三强放同频
		Collections.sort(sameEarfcnList, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP-o1.LteNcRSRP;
			}
		});
		int ydCount = 0;
		int ltCount = 0;
		int dxCount = 0;
		for (NC_LTE nc_LTE : sameEarfcnList){
			if (ydCount > 2) {//前三强
				break;
			}
			tlte[ydCount].LteNcPci = nc_LTE.LteNcPci;
			tlte[ydCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
			tlte[ydCount].LteNcRSRP = nc_LTE.LteNcRSRP;
			tlte[ydCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
			ydCount++;
		}
		
		//
		Collections.sort(nclteList, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP-o1.LteNcRSRP;
			}
		});
		for(NC_LTE nc_LTE : nclteList){
			int freqType = Func.getFreqType(nc_LTE.LteNcEarfcn);
			if (freqType == Func.YYS_YiDong) {
				if (ydCount < tlte.length) {
					tlte[ydCount].LteNcPci = nc_LTE.LteNcPci;
					tlte[ydCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
					tlte[ydCount].LteNcRSRP = nc_LTE.LteNcRSRP;
					tlte[ydCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
					ydCount++;
				}
			}else if (freqType == Func.YYS_LianTong) {
 				if (ltCount < lt_freq.length) {
					lt_freq[ltCount].LteNcPci = nc_LTE.LteNcPci;
					lt_freq[ltCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
					lt_freq[ltCount].LteNcRSRP = nc_LTE.LteNcRSRP;
					lt_freq[ltCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
					ltCount++;
				}
			}else if (freqType == Func.YYS_DianXin) {
				if (dxCount < dx_freq.length) {
					dx_freq[dxCount].LteNcPci = nc_LTE.LteNcPci;
					dx_freq[dxCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
					dx_freq[dxCount].LteNcRSRP = nc_LTE.LteNcRSRP;
					dx_freq[dxCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
					dxCount++;
				}
			}
		}
		
		nccount[0] = (byte)ydCount;
		sameEarfcnList.clear();
		nclteList.clear();
		
		
        /*for (int i = 0; i < nbcellnum; ++i)
		{
			data = new NC_LTE();
			data.LteNcPci = DataGeter.GetInt(values[pos++]);
			data.LteNcEarfcn = DataGeter.GetInt(values[pos++]);
			data.LteNcRSRP = DataGeter.GetInt(values[pos++]);
			data.LteNcRSRQ = DataGeter.GetInt(values[pos++]);
			
			int freqType = Func.getFreqType(data.LteNcEarfcn);
			if (freqType == Func.YYS_YiDong) {
				ncLteList.add(data);
			}else if (freqLteMap.get(data.LteNcEarfcn).LteNcRSRP < data.LteNcRSRP) {
				freqLteMap.get(data.LteNcEarfcn).LteNcPci = data.LteNcPci;
				freqLteMap.get(data.LteNcEarfcn).LteNcRSRP = data.LteNcRSRP;
				freqLteMap.get(data.LteNcEarfcn).LteNcRSRQ = data.LteNcRSRQ;
			} 
		}
		
		Collections.sort(ncLteList, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP - o1.LteNcRSRP;
			}
		});
		
		int ydCount = 0;
		Iterator<NC_LTE> it = ncLteList.iterator();
		//同频邻区放最前，最多取3组同频，rsrp逆序
		while(it.hasNext())
		{
			NC_LTE nc_LTE = it.next();
			if (nc_LTE.LteNcEarfcn == tsc.LteScEarfcn) {
				if (ydCount < 3) {
					tlte[ydCount] = nc_LTE;
					ydCount++;
				}
				it.remove();
			}
		}
		
		//剩余按rsrp大小取 （同频只有两组，则非同频按rsrp逆序取4组）
		for (NC_LTE nc_LTE :ncLteList)
		{
			if (ydCount < tlte.length) {
				tlte[ydCount] = nc_LTE;
				ydCount++;
			}
		}
		
		nccount[0] = (byte)ydCount;
		ncLteList.clear();
		
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
				if (ltCount < lt_freq.length)
				{
					lt_freq[ltCount] = ncItem;
					ltCount++;
				}
			}
			else if (type == Func.YYS_DianXin)
			{
				if (dxCount < dx_freq.length)
				{
					dx_freq[dxCount] = ncItem;
					dxCount++;
				}
			}
		}
		freqLteList.clear();
		freqLteMap.clear();*/

		// 初始值 转化为 常规值
		tsc.LteScRSRP = rsrpFormat(tsc.LteScRSRP);
		tsc.LteScRSRQ = rsrqFormat(tsc.LteScRSRQ);
		tsc.LteScSinrUL = sinrFormat(tsc.LteScSinrUL);
		tsc.LteScPHR = phrFormat(tsc.LteScPHR);

		for (int i = 0; i < ydCount; ++i)
		{
			tlte[i].LteNcRSRP = rsrpFormat(tlte[i].LteNcRSRP);
			tlte[i].LteNcRSRQ = rsrqFormat(tlte[i].LteNcRSRQ);
		}
		for (int i = 0; i < ltCount; ++i)
		{
			lt_freq[i].LteNcRSRP = rsrpFormat(lt_freq[i].LteNcRSRP);
			lt_freq[i].LteNcRSRQ = rsrqFormat(lt_freq[i].LteNcRSRQ);
		}
		for (int i = 0; i < dxCount; ++i)
		{
			dx_freq[i].LteNcRSRP = rsrpFormat(dx_freq[i].LteNcRSRP);
			dx_freq[i].LteNcRSRQ = rsrqFormat(dx_freq[i].LteNcRSRQ);
		}
		return true;

	}

	public boolean FillData_UEMro_ShenZhen(Object[] args)
	{
		String values[] = (String[]) args[0];
		// imsi经过加密，只取后16位
		String tmImsi = values[6].length() > 15 ? values[6].substring(0, 15) : "0";
		tsc.IMSI = Long.parseLong(tmImsi, 16);
		tsc.MmeUeS1apId = DataGeter.GetLong(values[10]);
		tsc.ENBId = DataGeter.GetInt(values[11]);
		tsc.CellId = DataGeter.GetLong(values[14]);
		tsc.Eci = tsc.CellId;
		tsc.beginTime = (int) (DataGeter.GetLong(values[16]) / 1000);
		tsc.beginTimems = (int) (DataGeter.GetLong(values[16]) % 1000);
		tsc.EventType = DataGeter.GetInt(values[17]) + "";
		tsc.LteScPHR = DataGeter.GetInt(values[18]);
		tsc.LteScSinrUL = DataGeter.GetInt(values[20]);
		tsc.LteScTadv = DataGeter.GetInt(values[21]);
		tsc.LteScAOA = DataGeter.GetInt(values[22]);
		tsc.Earfcn = DataGeter.GetInt(values[23]);
		tsc.LteScRSRP = DataGeter.GetInt(values[24]);
		tsc.LteScRSRQ = DataGeter.GetInt(values[25]);

		int nbcellnum = DataGeter.GetInt(values[26]);
		nbcellnum = nbcellnum > 6 ? 6 : nbcellnum;
		int pos = 27;
		for (int i = 0; i < nbcellnum; ++i)
		{
			tlte[i].LteNcPci = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcEarfcn = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcRSRP = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcRSRQ = DataGeter.GetInt(values[pos++]);
		}

		// 初始值 转化为 常规值
		tsc.LteScRSRP = rsrpFormat(tsc.LteScRSRP);
		tsc.LteScRSRQ = rsrqFormat(tsc.LteScRSRQ);
		tsc.LteScSinrUL = sinrFormat(tsc.LteScSinrUL);

		for (int i = 0; i < nbcellnum; ++i)
		{
			tlte[i].LteNcRSRP = rsrpFormat(tlte[i].LteNcRSRP);

			tlte[i].LteNcRSRQ = rsrqFormat(tlte[i].LteNcRSRQ);
		}

		return true;

	}

	public void FillData(DataAdapterReader dataAdapterReader) throws ParseException
	{
		tsc.cityID = dataAdapterReader.GetIntValue("cityID", -1);
		tsc.fileID = dataAdapterReader.GetIntValue("fileID", -1);

		Date d_beginTime = dataAdapterReader.GetDateValue("beginTime", new Date(1970, 1, 1));
		tsc.beginTime = (int) (d_beginTime.getTime() / 1000L);
		tsc.beginTimems = (int) (d_beginTime.getTime() % 1000L);

		double tmDouble = dataAdapterReader.GetDoubleValue("longitude", -1);
		if(tmDouble > 0)
		{
			tsc.longitude = (int)(tmDouble * 10000000);
		}
		
		tmDouble = dataAdapterReader.GetDoubleValue("latitude", -1);
		if(tmDouble > 0)
		{
			tsc.latitude = (int)(tmDouble * 10000000);
		}
		
		tsc.IMSI = dataAdapterReader.GetIntValue("IMSI", -1);

		tsc.TAC = dataAdapterReader.GetIntValue("TAC", -1);
		tsc.ENBId = dataAdapterReader.GetIntValue("ENBId", -1);
		tsc.UserLabel = dataAdapterReader.GetStrValue("UserLabel", "");

		tsc.CellId = dataAdapterReader.GetIntValue("CellId", -1);
		tsc.Eci = dataAdapterReader.GetIntValue("Eci", -1);
		if (tsc.CellId <= 255 && tsc.CellId > 0)
		{
			tsc.CellId = tsc.ENBId * 256L + tsc.CellId;
			tsc.Eci = tsc.CellId;
		}

		tsc.Earfcn = dataAdapterReader.GetIntValue("Earfcn", -1);
		tsc.SubFrameNbr = dataAdapterReader.GetIntValue("SubFrameNbr", -1);
		tsc.MmeCode = dataAdapterReader.GetIntValue("MmeCode", -1);
		tsc.MmeGroupId = dataAdapterReader.GetIntValue("MmeGroupId", -1);
		tsc.MmeUeS1apId = dataAdapterReader.GetLongValue("MmeUeS1apId", -1);
		tsc.Weight = dataAdapterReader.GetIntValue("Weight", -1);
		tsc.EventType = dataAdapterReader.GetStrValue("EventType", "");
		tsc.LteScRSRP = dataAdapterReader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		tsc.LteScRSRQ = dataAdapterReader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);
		tsc.LteScEarfcn = dataAdapterReader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		tsc.LteScPci = dataAdapterReader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);
		tsc.LteScBSR = dataAdapterReader.GetIntValue("LteScBSR", StaticConfig.Int_Abnormal);
		tsc.LteScRTTD = dataAdapterReader.GetIntValue("LteScRTTD", StaticConfig.Int_Abnormal);
		tsc.LteScTadv = dataAdapterReader.GetIntValue("LteScTadv", StaticConfig.Int_Abnormal);
		tsc.LteScAOA = dataAdapterReader.GetIntValue("LteScAOA", StaticConfig.Int_Abnormal);
		tsc.LteScPHR = dataAdapterReader.GetIntValue("LteScPHR", StaticConfig.Int_Abnormal);
		tsc.LteScSinrUL = dataAdapterReader.GetIntValue("LteScSinrUL", StaticConfig.Int_Abnormal);
		tsc.LteScRIP = dataAdapterReader.GetIntValue("LteScRIP", StaticConfig.Int_Abnormal);

		for (int i = 0; i < tsc.LteScPlrULQci.length; ++i)
		{
			tsc.LteScPlrULQci[i] = dataAdapterReader.GetDoubleValue("LteScPlrULQci" + (i + 1), StaticConfig.Double_Abnormal);
		}

		for (int i = 0; i < tsc.LteScPlrDLQci.length; ++i)
		{
			tsc.LteScPlrDLQci[i] = dataAdapterReader.GetDoubleValue("LteScPlrDLQci" + (i + 1), StaticConfig.Double_Abnormal);
		}

		tsc.LteScRI1 = dataAdapterReader.GetIntValue("LteScRI11", StaticConfig.Int_Abnormal);
		tsc.LteScRI2 = dataAdapterReader.GetIntValue("LteScRI12", StaticConfig.Int_Abnormal);
		tsc.LteScRI4 = dataAdapterReader.GetIntValue("LteScRI14", StaticConfig.Int_Abnormal);
		tsc.LteScRI8 = dataAdapterReader.GetIntValue("LteScRI18", StaticConfig.Int_Abnormal);

		tsc.LteScPUSCHPRBNum = dataAdapterReader.GetIntValue("LteScPUSCHPRBNum", -1);
		tsc.LteScPDSCHPRBNum = dataAdapterReader.GetIntValue("LteScPDSCHPRBNum", -1);

		tsc.imeiTac = dataAdapterReader.GetIntValue("imei", -1);

//		nccount[0] = (short) dataAdapterReader.GetIntValue("nccount1", -1);
//		nccount[1] = (short) dataAdapterReader.GetIntValue("nccount2", -1);
//		nccount[2] = (short) dataAdapterReader.GetIntValue("nccount3", -1);
//		nccount[3] = (short) dataAdapterReader.GetIntValue("nccount4", -1);

		NC_LTE data = null;
		List<NC_LTE> sameEarfcnList = new ArrayList<>();
		List<NC_LTE> nclteList = new ArrayList<>();
		for (int i = 0; i < tlte.length; ++i)
		{
			data = new NC_LTE();
			data.LteNcRSRP = dataAdapterReader.GetIntValue("LteNcRSRP" + (i + 1), StaticConfig.Int_Abnormal);
			data.LteNcRSRQ = dataAdapterReader.GetIntValue("LteNcRSRQ" + (i + 1), StaticConfig.Int_Abnormal);
			data.LteNcEarfcn = dataAdapterReader.GetIntValue("LteNcEarfcn" + (i + 1), StaticConfig.Int_Abnormal);
			data.LteNcPci = dataAdapterReader.GetIntValue("LteNcPci" + (i + 1), StaticConfig.Int_Abnormal);
			
			if (data.LteNcEarfcn == tsc.LteScEarfcn){
				sameEarfcnList.add(data);
			}else {
				nclteList.add(data);
			}
		}
		
		//前三强放同频
		Collections.sort(sameEarfcnList, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP-o1.LteNcRSRP;
			}
		});
		int ydCount = 0;
		int ltCount = 0;
		int dxCount = 0;
		for (NC_LTE nc_LTE : sameEarfcnList){
			if (ydCount > 2) {//前三强
				break;
			}
			tlte[ydCount].LteNcPci = nc_LTE.LteNcPci;
			tlte[ydCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
			tlte[ydCount].LteNcRSRP = nc_LTE.LteNcRSRP;
			tlte[ydCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
			ydCount++;
		}
		
		//
		Collections.sort(nclteList, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP-o1.LteNcRSRP;
			}
		});
		for(NC_LTE nc_LTE : nclteList){
			int freqType = Func.getFreqType(nc_LTE.LteNcEarfcn);
			if (freqType == Func.YYS_YiDong) {
				if (ydCount < tlte.length) {
					tlte[ydCount].LteNcPci = nc_LTE.LteNcPci;
					tlte[ydCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
					tlte[ydCount].LteNcRSRP = nc_LTE.LteNcRSRP;
					tlte[ydCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
					ydCount++;
				}
			}else if (freqType == Func.YYS_LianTong) {
 				if (ltCount < lt_freq.length) {
					lt_freq[ltCount].LteNcPci = nc_LTE.LteNcPci;
					lt_freq[ltCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
					lt_freq[ltCount].LteNcRSRP = nc_LTE.LteNcRSRP;
					lt_freq[ltCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
					ltCount++;
				}
			}else if (freqType == Func.YYS_DianXin) {
				if (dxCount < dx_freq.length) {
					dx_freq[dxCount].LteNcPci = nc_LTE.LteNcPci;
					dx_freq[dxCount].LteNcEarfcn = nc_LTE.LteNcEarfcn;
					dx_freq[dxCount].LteNcRSRP = nc_LTE.LteNcRSRP;
					dx_freq[dxCount].LteNcRSRQ = nc_LTE.LteNcRSRQ;
					dxCount++;
				}
			}
		}
		
		nccount[0] = (byte)ydCount;
		nccount[1] = (byte)(ltCount + dxCount);
		sameEarfcnList.clear();
		nclteList.clear();
		
		// 初始值 转化为 常规值
		tsc.LteScRSRP = rsrpFormat(tsc.LteScRSRP);
		tsc.LteScRSRQ = rsrqFormat(tsc.LteScRSRQ);
		tsc.LteScSinrUL = sinrFormat(tsc.LteScSinrUL);
		tsc.LteScPHR = phrFormat(tsc.LteScPHR);

		for (int i = 0; i < ydCount; ++i)
		{
			tlte[i].LteNcRSRP = rsrpFormat(tlte[i].LteNcRSRP);
			tlte[i].LteNcRSRQ = rsrqFormat(tlte[i].LteNcRSRQ);
		}
		for (int i = 0; i < ltCount; ++i)
		{
			lt_freq[i].LteNcRSRP = rsrpFormat(lt_freq[i].LteNcRSRP);
			lt_freq[i].LteNcRSRQ = rsrqFormat(lt_freq[i].LteNcRSRQ);
		}
		for (int i = 0; i < dxCount; ++i)
		{
			dx_freq[i].LteNcRSRP = rsrpFormat(dx_freq[i].LteNcRSRP);
			dx_freq[i].LteNcRSRQ = rsrqFormat(dx_freq[i].LteNcRSRQ);
		}
	}

	private int rsrpFormat(int origRsrp){
		if (origRsrp >= 0)
			return origRsrp - 141;
		else return origRsrp;
	}

	private int rsrqFormat(int origRsrq){
		if (origRsrq == 255 || origRsrq == StaticConfig.Int_Abnormal)
			return StaticConfig.Int_Abnormal;
		else return (origRsrq - 40) / 2;
	}

	private int sinrFormat(int origSinr){
		if (origSinr == 255 || origSinr == StaticConfig.Int_Abnormal)
			return StaticConfig.Int_Abnormal;
		else return origSinr - 11;
	}
	
	private int phrFormat(int origPhr){
		if (origPhr == 255 || origPhr == StaticConfig.Int_Abnormal) {
			return StaticConfig.Int_Abnormal;
		}
		else return origPhr - 23;
	}

	@Override
	public boolean fillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {

		switch (dataAdapterReader.getParseItem().getParseType()){

			case "MRO-SRC":
				FillData(dataAdapterReader);
				break;

			case "MDT-SRC-IMM":
			case "MDT-SRC-IMM-HUAWEI":
			case "MDT-SRC-IMM-ZTE":
				FillIMMData(dataAdapterReader);
				break;

			case "MDT-SRC-LOG":
			case "MDT-SRC-LOG-HUAWEI":
			case "MDT-SRC-LOG-ZTE":
				FillLOGData(dataAdapterReader);
				break;

			default:
				throw new IllegalArgumentException();

		}
		return true;
	}

	@Override
	public void fromFormatedString(String value) throws Exception {

	}

	@Override
	public String toFormatedString() {
		return null;
	}
}