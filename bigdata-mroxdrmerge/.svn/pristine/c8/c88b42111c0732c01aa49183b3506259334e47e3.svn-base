package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

import cn.mastercom.bigdata.util.DataGeter;

public class SC_FRAME implements Serializable
{
	public int Earfcn;
	public short SubFrame;
	public short LteScRIP;
	
	public SC_FRAME()
	{
		Clear();
	}

	public void Clear()
	{
		Earfcn = StaticConfig.Int_Abnormal;
		SubFrame = StaticConfig.TinyInt_Abnormal;
		LteScRIP = StaticConfig.Short_Abnormal;
	}
	
	public String GetData()
	{
		StringBuffer res = new StringBuffer();
		
		res.append(Earfcn);
		res.append(StaticConfig.DataSlipter);
		res.append((int)SubFrame);
		res.append(StaticConfig.DataSlipter);
		res.append((int)LteScRIP);

        return 	res.toString();		
	}
	
	public void FillData(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		Earfcn = DataGeter.GetInt(values[i++]); 
		SubFrame = DataGeter.GetTinyInt(values[i++]);
		LteScRIP = DataGeter.GetShort(values[i++]);
		
		args[1] = i;
	}
	
}
