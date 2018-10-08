package cn.mastercom.bigdata.StructData;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataGeter;

public class MroOrigDataMT implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

	public Date beginTime;// yyyy-MM-dd HH:mm:ss.000
	public int ENBId;
	public String UserLabel;
	public int CellId;
	public int Earfcn;
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public String EventType;
	public int LteScRSRP;
	public int LteNcRSRP;
	public int LteScRSRQ;
	public int LteNcRSRQ;
	public int LteScEarfcn;
	public int LteScPci;
	public int LteNcEarfcn;
	public int LteNcPci;
	public int GsmNcellCarrierRSSI;
	public int GsmNcellBcch;
	public int GsmNcellNcc;
	public int GsmNcellBcc;
	public int TdsPccpchRSCP;
	public int TdsNcellUarfcn;
	public int TdsCellParameterId;
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public double LteScRIP;
	public int LteScSinrUL;
	public double[] LteScPlrULQci;
	public double[] LteScPlrDLQci;
	public int LteScRI1;
	public int LteScRI2;
	public int LteScRI4;
	public int LteScRI8;
	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	// neimeng
	public int GsmNcellLac;
	public int GsmNcellCi;
	public int LteNcEnodeb;
	public int LteNcCid;
	
	public int longitude;
	public int latitude;
	
	private String tmStr = "";

	public MroOrigDataMT()
	{
		beginTime = new Date();
		ENBId = StaticConfig.Int_Abnormal;
		UserLabel = "";
		CellId = StaticConfig.Int_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		SubFrameNbr = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		EventType = "";
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteNcRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		LteNcRSRQ = StaticConfig.Int_Abnormal;
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteNcEarfcn = StaticConfig.Int_Abnormal;
		LteNcPci = StaticConfig.Int_Abnormal;
		GsmNcellCarrierRSSI = StaticConfig.Int_Abnormal;
		GsmNcellBcch = StaticConfig.Int_Abnormal;
		GsmNcellNcc = StaticConfig.Int_Abnormal;
		GsmNcellBcc = StaticConfig.Int_Abnormal;
		TdsPccpchRSCP = StaticConfig.Int_Abnormal;
		TdsNcellUarfcn = StaticConfig.Int_Abnormal;
		TdsCellParameterId = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScRIP = StaticConfig.Double_Abnormal;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LteScPlrULQci = new double[9];
		for (int i = 0; i < LteScPlrULQci.length; ++i)
		{
			LteScPlrULQci[i] = StaticConfig.Int_Abnormal;
		}

		LteScPlrDLQci = new double[9];
		for (int i = 0; i < LteScPlrDLQci.length; ++i)
		{
			LteScPlrDLQci[i] = StaticConfig.Int_Abnormal;
		}

		LteScRI1 = StaticConfig.Int_Abnormal;
		LteScRI2 = StaticConfig.Int_Abnormal;
		LteScRI4 = StaticConfig.Int_Abnormal;
		LteScRI8 = StaticConfig.Int_Abnormal;
		LteScPUSCHPRBNum = StaticConfig.Int_Abnormal;
		LteScPDSCHPRBNum = StaticConfig.Int_Abnormal;
		LteSceNBRxTxTimeDiff = StaticConfig.Int_Abnormal;

		GsmNcellLac = StaticConfig.Int_Abnormal;
		GsmNcellCi = StaticConfig.Int_Abnormal;
		LteNcEnodeb = StaticConfig.Int_Abnormal;
		LteNcCid = StaticConfig.Int_Abnormal;
		
		longitude = StaticConfig.Int_Abnormal;
		latitude = StaticConfig.Int_Abnormal;
	}

	public boolean FillData(DataAdapterReader reader) throws ParseException
	{
		beginTime = reader.GetDateValue("beginTime", null);
		ENBId = reader.GetIntValue("ENBId", -1);
		UserLabel = reader.GetStrValue("UserLabel", "");
		// CellId = reader.GetIntValue("CellId", -1);
		tmStr = reader.GetStrValue("CellId", "0");
		if (tmStr.indexOf(":") > 0)
		{
			CellId = DataGeter.GetInt(tmStr.substring(0, tmStr.indexOf(":")));
		}
		else if (tmStr.indexOf("-") > 0)
		{
			CellId = DataGeter.GetInt(tmStr.substring(tmStr.indexOf("-") + 1));
		}
		else
		{
			CellId = DataGeter.GetInt(tmStr);
		}

		if (CellId > 256)
		{
			CellId = CellId % 256;
		}
		Earfcn = reader.GetIntValue("Earfcn", StaticConfig.Int_Abnormal);
		SubFrameNbr = reader.GetIntValue("SubFrameNbr", StaticConfig.Int_Abnormal);
		MmeCode = reader.GetIntValue("MmeCode", StaticConfig.Int_Abnormal);
		MmeGroupId = reader.GetIntValue("MmeGroupId", StaticConfig.Int_Abnormal);
		MmeUeS1apId = reader.GetLongValue("MmeUeS1apId", StaticConfig.Int_Abnormal);
		Weight = reader.GetIntValue("Weight", StaticConfig.Int_Abnormal);
		EventType = reader.GetStrValue("EventType", "MRO");
		LteScRSRP = reader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		LteNcRSRP = reader.GetIntValue("LteNcRSRP", StaticConfig.Int_Abnormal);
		LteScRSRQ = reader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);
		LteNcRSRQ = reader.GetIntValue("LteNcRSRQ", StaticConfig.Int_Abnormal);
		LteScEarfcn = reader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		LteScPci = reader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);
		LteNcEarfcn = reader.GetIntValue("LteNcEarfcn", StaticConfig.Int_Abnormal);
		LteNcPci = reader.GetIntValue("LteNcPci", StaticConfig.Int_Abnormal);
		GsmNcellCarrierRSSI = reader.GetIntValue("GsmNcellCarrierRSSI", StaticConfig.Int_Abnormal);
		GsmNcellBcch = reader.GetIntValue("GsmNcellBcch", StaticConfig.Int_Abnormal);
		GsmNcellNcc = reader.GetIntValue("GsmNcellNcc", StaticConfig.Int_Abnormal);
		GsmNcellBcc = reader.GetIntValue("GsmNcellBcc", StaticConfig.Int_Abnormal);
		TdsPccpchRSCP = reader.GetIntValue("TdsPccpchRSCP", StaticConfig.Int_Abnormal);
		TdsNcellUarfcn = reader.GetIntValue("TdsNcellUarfcn", StaticConfig.Int_Abnormal);
		TdsCellParameterId = reader.GetIntValue("TdsCellParameterId", StaticConfig.Int_Abnormal);
		LteScBSR = reader.GetIntValue("LteScBSR", StaticConfig.Int_Abnormal);
		LteScRTTD = reader.GetIntValue("LteScRTTD", StaticConfig.Int_Abnormal);
		LteScTadv = reader.GetIntValue("LteScTadv", StaticConfig.Int_Abnormal);
		LteScAOA = reader.GetIntValue("LteScAOA", StaticConfig.Int_Abnormal);
		LteScPHR = reader.GetIntValue("LteScPHR", StaticConfig.Int_Abnormal);
		LteScRIP = reader.GetDoubleValue("LteScRIP", StaticConfig.Double_Abnormal);
		LteScSinrUL = reader.GetIntValue("LteScSinrUL", StaticConfig.Int_Abnormal);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			GsmNcellLac = reader.GetIntValue("GsmNcellLac", StaticConfig.Int_Abnormal);
			GsmNcellCi = reader.GetIntValue("GsmNcellCi", StaticConfig.Int_Abnormal);
			LteNcEnodeb = reader.GetIntValue("LteNcEnodeb", StaticConfig.Int_Abnormal);
			LteNcCid = reader.GetIntValue("LteNcCid", StaticConfig.Int_Abnormal);
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.MroDetail))
		{

			for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
			{
				LteScPlrULQci[ii] = reader.GetDoubleValue("LteScPlrULQci" + (ii + 1), StaticConfig.Double_Abnormal);
			}

			for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
			{
				LteScPlrDLQci[ii] = reader.GetDoubleValue("LteScPlrDLQci" + (ii + 1), StaticConfig.Double_Abnormal);
			}

			LteScRI1 = reader.GetIntValue("LteScRI1", StaticConfig.Int_Abnormal);
			LteScRI2 = reader.GetIntValue("LteScRI2", StaticConfig.Int_Abnormal);
			LteScRI4 = reader.GetIntValue("LteScRI4", StaticConfig.Int_Abnormal);
			LteScRI8 = reader.GetIntValue("LteScRI8", StaticConfig.Int_Abnormal);
			LteScPUSCHPRBNum = reader.GetIntValue("LteScPUSCHPRBNum", StaticConfig.Int_Abnormal);
			LteScPDSCHPRBNum = reader.GetIntValue("LteScPDSCHPRBNum", StaticConfig.Int_Abnormal);
			LteSceNBRxTxTimeDiff = reader.GetIntValue("LteSceNBRxTxTimeDiff", StaticConfig.Int_Abnormal);
		}
		double tmp;
		longitude = (int) ((tmp = reader.GetDoubleValue("Longitude", StaticConfig.Double_Abnormal)) > 0 ? tmp * 10000000 : StaticConfig.Int_Abnormal);
		latitude = (int) ((tmp = reader.GetDoubleValue("Latitude", StaticConfig.Double_Abnormal)) > 0 ? tmp * 10000000 : StaticConfig.Int_Abnormal);

		// 初始值 转化为 常规值
		LteScPHR = phrFormat(LteScPHR);
		if (LteScRIP != StaticConfig.Double_Abnormal)
		{
			LteScRIP = LteScRIP * 0.1 - 126.1D;
		}
		if (GsmNcellCarrierRSSI >= 0)
			GsmNcellCarrierRSSI -= 101;
		if (TdsPccpchRSCP >= 0)
			TdsPccpchRSCP -= 116;
		LteNcRSRP = rsrpFormat(LteNcRSRP);
		LteNcRSRQ = rsrqFormat(LteNcRSRQ);
		LteScRSRP = rsrpFormat(LteScRSRP);
		LteScRSRQ = rsrqFormat(LteScRSRQ);
		LteScSinrUL = sinrFormat(LteScSinrUL);

//		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing))
//		{
//			if (LteScRSRP == -141)// 辽宁存在大量rsrp=-141的情况。这类数据异常，不参与计算。
//			{
//				return false;
//			}
//		}

		return true;
	}
	
	private int rsrpFormat(int origRsrp){
		if (origRsrp >= 0)
			return origRsrp - 141;
		else return origRsrp;
	}

	private int rsrqFormat(int origRsrq){
		if (origRsrq >= 0)
			return (origRsrq - 40) / 2;
		else return origRsrq;
	}

	private int sinrFormat(int origSinr){
		if (origSinr >= 0)
			return origSinr - 11;
		else return origSinr;
	}
	
	private int phrFormat(int origPhr){
		if (origPhr == 255 || origPhr == StaticConfig.Int_Abnormal) {
			return StaticConfig.Int_Abnormal;
		}
		else return origPhr - 23;
	}

}