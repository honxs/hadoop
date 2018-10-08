package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

import cn.mastercom.bigdata.util.DataGeter;

public class NC_LTE implements Serializable
{
	public int LteNcRSRP;
	public int LteNcRSRQ;
	public int LteNcEarfcn;
	public int LteNcPci;
	public long LteNcEci;
	//neimeng 新增LteNcEnodeb 
	public int LteNcEnodeb;
	public int LteNcCid;
    
	public NC_LTE(){};
	public NC_LTE(int lteNcRSRP, int lteNcRSRQ, int lteNcEarfcn, int lteNcPci)
	{
		super();
		LteNcRSRP = lteNcRSRP;
		LteNcRSRQ = lteNcRSRQ;
		LteNcEarfcn = lteNcEarfcn;
		LteNcPci = lteNcPci;
	}

	public void Clear()
	{
		LteNcRSRP = StaticConfig.Int_Abnormal;
		LteNcRSRQ = StaticConfig.Int_Abnormal;
		LteNcEarfcn = StaticConfig.Int_Abnormal;
		LteNcPci = StaticConfig.Int_Abnormal;
		LteNcEnodeb = StaticConfig.Int_Abnormal;
		LteNcCid = StaticConfig.Int_Abnormal;
		LteNcEci = StaticConfig.Long_Abnormal; 
	}
	
	public String GetData()
	{
		StringBuffer res = new StringBuffer();
		
		res.append(LteNcRSRP);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcRSRQ);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcEarfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcPci);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcEci);

        return 	res.toString();		
	}
	
	public String GetDataAll()
	{
		StringBuffer res = new StringBuffer();
		
		res.append(LteNcEnodeb);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcCid);

        return 	res.toString();		
	} 
	
	public void FillData(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		LteNcRSRP = DataGeter.GetInt(values[i++]);
		LteNcRSRQ = DataGeter.GetInt(values[i++]);
		LteNcEarfcn = DataGeter.GetInt(values[i++]);
		LteNcPci = DataGeter.GetInt(values[i++]);
		LteNcEci = DataGeter.GetLong(values[i++]);
		
		args[1] = i;
	}
	
	public void FillDataAll(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		LteNcEnodeb = DataGeter.GetInt(values[i++]);
		LteNcCid = DataGeter.GetInt(values[i++]);
		
		args[1] = i;
	}
	
}
