package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

import cn.mastercom.bigdata.util.DataGeter;

public class NC_TDS implements Serializable
{
	public int TdsPccpchRSCP;
	public int TdsNcellUarfcn;
	public int TdsCellParameterId;

	public void Clear()
	{
		TdsPccpchRSCP = StaticConfig.Int_Abnormal;
		TdsNcellUarfcn = StaticConfig.Int_Abnormal;
		TdsCellParameterId = StaticConfig.Int_Abnormal;
	}
	
	public String GetData()
	{
		StringBuffer res = new StringBuffer();
		
		res.append(TdsPccpchRSCP);
		res.append(StaticConfig.DataSlipter);
		res.append(TdsNcellUarfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(TdsCellParameterId);

        return 	res.toString();		
	}
	
	public void FillData(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		TdsPccpchRSCP = DataGeter.GetInt(values[i++]);
		TdsNcellUarfcn = DataGeter.GetInt(values[i++]);
		TdsCellParameterId = DataGeter.GetInt(values[i++]);
		
		args[1] = i;
	}
	
}