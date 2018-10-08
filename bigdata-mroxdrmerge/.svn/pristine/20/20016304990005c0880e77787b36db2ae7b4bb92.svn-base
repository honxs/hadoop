package cn.mastercom.bigdata.evt.locall.stat;

import cn.mastercom.bigdata.StructData.StaticConfig;

public class EventDataStruct
{
	public int sampleType;
	public String strvalue[];
	public double fvalue[];
	
	public EventDataStruct()
	{
		strvalue = new String[2];
		for (int i = 0; i < strvalue.length; i++)
		{
			strvalue[i] = "";
		}
		
		fvalue = new double[20];
		for (int f = 0; f < fvalue.length; f++)
		{
			fvalue[f] = StaticConfig.Int_Abnormal;
		}
	}
	
	/**
	 * TODO 2017-11-13 有空需要改，因为这个不能统计负数的值，如果要改的话，需要各个指标都要对比
	 * @param data
	 */
	public void stat(EventDataStruct data)
	{	
		for (int i = 0; i < data.fvalue.length; i++)
		{
			if(data.fvalue[i] >= 0)
			{
				if(fvalue[i]<0){
					fvalue[i] = data.fvalue[i];
				}else{
					fvalue[i] += data.fvalue[i];
				}
			}
		}	

	}
	
	public void toString(StringBuffer sb)
	{

		
		for (int i = 0; i < fvalue.length; i++)
		{
			if(i ==  fvalue.length - 1)
			{
				sb.append(fvalue[i]);
			}
			else 
			{
				sb.append(fvalue[i]);sb.append("\t");
			}	
		}
	}
	
	public void toString(StringBuffer sb,int type)
	{
		for (int i = 0; i < strvalue.length; i++)
		{
			sb.append(strvalue[i]);sb.append("\t");
		}
		
		for (int i = 0; i < fvalue.length; i++)
		{
			if(i ==  fvalue.length - 1)
			{
				sb.append(fvalue[i]);;
			}
			else 
			{
				sb.append(fvalue[i]);sb.append("\t");
			}	
		}
	}
	
	
}
