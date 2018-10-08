package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

import cn.mastercom.bigdata.util.DataGeter;

public class NC_GSM implements Serializable
{
	public int GsmNcellCarrierRSSI;
	public int GsmNcellBcch;
	public int GsmNcellBsic; // NCC(3bit)+BCC(3bit)
	public int GsmNcellLac;
	public int GsmNcellCi;

	public void Clear()
	{
		GsmNcellCarrierRSSI = StaticConfig.Int_Abnormal;
		GsmNcellBcch = StaticConfig.Int_Abnormal;
		GsmNcellBsic = StaticConfig.Int_Abnormal;
		GsmNcellLac = StaticConfig.Int_Abnormal;
		GsmNcellCi = StaticConfig.Int_Abnormal;
	}
	
	public String GetData()
	{
		StringBuffer res = new StringBuffer();
		
		res.append(GsmNcellCarrierRSSI);
		res.append(StaticConfig.DataSlipter);
		res.append(GsmNcellBcch);
		res.append(StaticConfig.DataSlipter);
		res.append(GsmNcellBsic);
		
		res.append(StaticConfig.DataSlipter);
		res.append(GsmNcellLac);
		res.append(StaticConfig.DataSlipter);
		res.append(GsmNcellCi);
		
        return 	res.toString();		
	}
	
//	public String GetDataAll()
//	{
//		StringBuffer res = new StringBuffer();
//		
//		res.append(GsmNcellCarrierRSSI);
//		res.append(StaticConfig.DataSlipter);
//		res.append(GsmNcellBcch);
//		res.append(StaticConfig.DataSlipter);
//		res.append(GsmNcellBsic);
//		
//		res.append(StaticConfig.DataSlipter);
//		res.append(GsmNcellLac);
//		res.append(StaticConfig.DataSlipter);
//		res.append(GsmNcellCi);
//		
//        return 	res.toString();		
//	}
//	

	public void FillData(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		GsmNcellCarrierRSSI = DataGeter.GetInt(values[i++]);
		GsmNcellBcch = DataGeter.GetInt(values[i++]);
		GsmNcellBsic = DataGeter.GetInt(values[i++]);
		GsmNcellLac = DataGeter.GetInt(values[i++]);
		GsmNcellCi = DataGeter.GetInt(values[i++]);
		args[1] = i;
	}
	
//	public void FillDataAll(Object[] args)
//	{	
//		String[] values = (String[])args[0];
//		Integer i = (Integer)args[1];
//		
//		GsmNcellCarrierRSSI = DataGeter.GetInt(values[i++]);
//		GsmNcellBcch = DataGeter.GetInt(values[i++]);
//		GsmNcellBsic = DataGeter.GetInt(values[i++]);
//		GsmNcellLac = DataGeter.GetInt(values[i++]);
//		GsmNcellCi = DataGeter.GetInt(values[i++]);
//		args[1] = i;
//	}
	
}
