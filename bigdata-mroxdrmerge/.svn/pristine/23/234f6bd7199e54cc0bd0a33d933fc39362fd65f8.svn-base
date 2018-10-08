package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

import cn.mastercom.bigdata.util.DataGeter;

public class SIGNAL_MR_SC implements Serializable
{
	public int cityID;
	public int fileID;
	public int beginTime;
	public int beginTimems;
	public int longitude;
	public int latitude;
	public long IMSI;
	public int TAC;
	public int ENBId;
	public String UserLabel = "";
	public long CellId;
	public long Eci;
	public int Earfcn; // FDD鐨凟ARFCN浠�0~35999,TDD鐨凟ARFCN浠�36000~65531
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public String EventType = "";//mro mre mdt
	public int LteScRSRP;
	public int LteScRSRQ;
	public int LteScEarfcn;
	public int LteScPci;// 0~503
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public int LteScSinrUL;
	public double LteScRIP;
	public double[] LteScPlrULQci;// 9
	public double[] LteScPlrDLQci;// 9
	public int LteScRI1;
	public int LteScRI2;
	public int LteScRI4;
	public int LteScRI8;
	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	public int imeiTac;
	public String Msisdn ="";
//	private StringBuffer sb = new StringBuffer();

	public SIGNAL_MR_SC()
	{
		Clear();
	}

	public void Clear()
	{
		cityID = -1;
		fileID = -1;
		beginTime = StaticConfig.Int_Abnormal;
		beginTimems = StaticConfig.Int_Abnormal;
		longitude = StaticConfig.Int_Abnormal;
		latitude = StaticConfig.Int_Abnormal;
		IMSI = StaticConfig.Long_Abnormal;
		TAC = StaticConfig.Int_Abnormal;
		ENBId = StaticConfig.Int_Abnormal;
		CellId = StaticConfig.Int_Abnormal;
		UserLabel = StaticConfig.String_Abnormal;;
		Eci = StaticConfig.Int_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal; // FDD鐨凟ARFCN浠�0~35999,TDD鐨凟ARFCN浠�36000~65531
		SubFrameNbr = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		EventType = StaticConfig.String_Abnormal;//mro mre mdt
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LteScRIP = StaticConfig.Double_Abnormal;
		LteScRI1 = StaticConfig.Int_Abnormal;
		LteScRI2 = StaticConfig.Int_Abnormal;
		LteScRI4 = StaticConfig.Int_Abnormal;
		LteScRI8 = StaticConfig.Int_Abnormal;
		LteScPUSCHPRBNum = StaticConfig.Int_Abnormal;
		LteScPDSCHPRBNum = StaticConfig.Int_Abnormal;
		LteSceNBRxTxTimeDiff = StaticConfig.Int_Abnormal;
		imeiTac = StaticConfig.Int_Abnormal;
		Msisdn = StaticConfig.String_Abnormal;
		EventType = StaticConfig.String_Abnormal;
		
		LteScPlrULQci = new double[9];
		LteScPlrDLQci = new double[9];

		for (int ik = 0; ik < 9; ik++)
		{
			LteScPlrULQci[ik] = StaticConfig.Double_Abnormal;
			LteScPlrDLQci[ik] = StaticConfig.Double_Abnormal;
		}

	};

	public String GetData()
	{
		StringBuffer res = new StringBuffer();

		res.append(cityID);
		res.append(StaticConfig.DataSlipter);
		res.append(fileID);
		res.append(StaticConfig.DataSlipter);
		res.append(beginTime);
		res.append(StaticConfig.DataSlipter);
		res.append(beginTimems);
		res.append(StaticConfig.DataSlipter);
		res.append(longitude);
		res.append(StaticConfig.DataSlipter);
		res.append(latitude);
		res.append(StaticConfig.DataSlipter);
		res.append(IMSI);
		res.append(StaticConfig.DataSlipter);
		res.append(TAC);
		res.append(StaticConfig.DataSlipter);
		res.append(ENBId);
		res.append(StaticConfig.DataSlipter);
		res.append(UserLabel);
		res.append(StaticConfig.DataSlipter);
		res.append(CellId);
		res.append(StaticConfig.DataSlipter);
		res.append(Eci);
		res.append(StaticConfig.DataSlipter);
		res.append(Earfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(SubFrameNbr);
		res.append(StaticConfig.DataSlipter);
		res.append(MmeCode);
		res.append(StaticConfig.DataSlipter);
		res.append(MmeGroupId);
		res.append(StaticConfig.DataSlipter);
		res.append(MmeUeS1apId);
		res.append(StaticConfig.DataSlipter);
		res.append(Weight);
		res.append(StaticConfig.DataSlipter);
		res.append(EventType);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRP);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRQ);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScEarfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPci);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScBSR);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRTTD);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScTadv);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScAOA);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPHR);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScSinrUL);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRIP);
		res.append(StaticConfig.DataSlipter);

		for (double data : LteScPlrULQci)
		{
			res.append(data);
			res.append(StaticConfig.DataSlipter);
		}

		for (double data : LteScPlrDLQci)
		{
			res.append(data);
			res.append(StaticConfig.DataSlipter);
		}

		res.append(LteScRI1);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRI2);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRI4);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRI8);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPUSCHPRBNum);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPDSCHPRBNum);
		res.append(StaticConfig.DataSlipter);
		res.append(imeiTac);

		return res.toString();
	}

	public void FillDataOld(Object[] args)
	{
		String[] values = (String[]) args[0];
		Integer i = (Integer) args[1];

		cityID = Integer.parseInt(values[i++]);
		fileID = Integer.parseInt(values[i++]);
		beginTime = Integer.parseInt(values[i++]);
		beginTimems = Integer.parseInt(values[i++]);
		longitude = Integer.parseInt(values[i++]);
		latitude = Integer.parseInt(values[i++]);
		IMSI = Long.parseLong(values[i++]);
		TAC = Integer.parseInt(values[i++]);
		ENBId = Integer.parseInt(values[i++]);
		UserLabel = values[i++];
		CellId = Long.parseLong(values[i++]);
		Eci = Long.parseLong(values[i++]);
		Earfcn = Integer.parseInt(values[i++]);
		SubFrameNbr = Integer.parseInt(values[i++]);
		MmeCode = Integer.parseInt(values[i++]);
		MmeGroupId = Integer.parseInt(values[i++]);
		MmeUeS1apId = Long.parseLong(values[i++]);
		Weight = Integer.parseInt(values[i++]);
		EventType = values[i++];
		LteScRSRP = Integer.parseInt(values[i++]);
		LteScRSRQ = Integer.parseInt(values[i++]);
		LteScEarfcn = Integer.parseInt(values[i++]);
		LteScPci = Integer.parseInt(values[i++]);// 0~503
		LteScBSR = Integer.parseInt(values[i++]);
		LteScRTTD = Integer.parseInt(values[i++]);
		LteScTadv = Integer.parseInt(values[i++]);
		LteScAOA = Integer.parseInt(values[i++]);
		LteScPHR = Integer.parseInt(values[i++]);
		LteScSinrUL = Integer.parseInt(values[i++]);
		LteScRIP = Double.parseDouble(values[i++]);

		for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
		{
			LteScPlrULQci[ii] = Double.parseDouble(values[i++]);
		}

		for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
		{
			LteScPlrDLQci[ii] = Double.parseDouble(values[i++]);
		}

		LteScRI1 = Integer.parseInt(values[i++]);
		LteScRI2 = Integer.parseInt(values[i++]);
		LteScRI4 = Integer.parseInt(values[i++]);
		LteScRI8 = Integer.parseInt(values[i++]);
		LteScPUSCHPRBNum = Integer.parseInt(values[i++]);
		LteScPDSCHPRBNum = Integer.parseInt(values[i++]);
		imeiTac = Integer.parseInt(values[i++]);

		args[1] = i;
	}

	public void FillData(Object[] args)
	{
		String[] values = (String[]) args[0];
		Integer i = (Integer) args[1];

		cityID = DataGeter.GetInt(values[i++]);
		fileID = DataGeter.GetInt(values[i++]);
		beginTime = DataGeter.GetInt(values[i++]);
		beginTimems = DataGeter.GetInt(values[i++]);
		longitude = DataGeter.GetInt(values[i++]);
		latitude = DataGeter.GetInt(values[i++]);
		IMSI = DataGeter.GetLong(values[i++]);
		TAC = DataGeter.GetInt(values[i++]);
		ENBId = DataGeter.GetInt(values[i++]);
		UserLabel = DataGeter.GetString(values[i++]);
		CellId = DataGeter.GetLong(values[i++]);
		Eci = DataGeter.GetLong(values[i++]);
		Earfcn = DataGeter.GetInt(values[i++]);
		SubFrameNbr = DataGeter.GetInt(values[i++]);
		MmeCode = DataGeter.GetInt(values[i++]);
		MmeGroupId = DataGeter.GetInt(values[i++]);
		MmeUeS1apId = DataGeter.GetLong(values[i++]);
		Weight = DataGeter.GetInt(values[i++]);
		EventType = DataGeter.GetString(values[i++]);
		LteScRSRP = DataGeter.GetInt(values[i++]);
		LteScRSRQ = DataGeter.GetInt(values[i++]);
		LteScEarfcn = DataGeter.GetInt(values[i++]);
		LteScPci = DataGeter.GetInt(values[i++]);// 0~503
		LteScBSR = DataGeter.GetInt(values[i++]);
		LteScRTTD = DataGeter.GetInt(values[i++]);
		LteScTadv = DataGeter.GetInt(values[i++]);
		LteScAOA = DataGeter.GetInt(values[i++]);
		LteScPHR = DataGeter.GetInt(values[i++]);
		LteScSinrUL = DataGeter.GetInt(values[i++]);
		LteScRIP = DataGeter.GetDouble(values[i++]);

		for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
		{
			LteScPlrULQci[ii] = DataGeter.GetDouble(values[i++]);
		}

		for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
		{
			LteScPlrDLQci[ii] = DataGeter.GetDouble(values[i++]);
		}

		LteScRI1 = DataGeter.GetInt(values[i++]);
		LteScRI2 = DataGeter.GetInt(values[i++]);
		LteScRI4 = DataGeter.GetInt(values[i++]);
		LteScRI8 = DataGeter.GetInt(values[i++]);
		LteScPUSCHPRBNum = DataGeter.GetInt(values[i++]);
		LteScPDSCHPRBNum = DataGeter.GetInt(values[i++]);
		imeiTac = DataGeter.GetInt(values[i++]);

		args[1] = i;
	}
	
}
