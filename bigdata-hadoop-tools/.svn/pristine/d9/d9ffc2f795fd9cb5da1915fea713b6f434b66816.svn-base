package cn.mastercom.bigdata.util.spark;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.util.IDataOutputer;

public class DataOutputMng implements IDataOutputer
{
	private List<IDataOutputer> outputerList = new ArrayList<IDataOutputer>();
	
	public DataOutputMng(List<IDataOutputer> outputerList)
	{
		this.outputerList = outputerList;
	}
	

	@Override
	public int pushData(int dataType, String value)
	{
		for(IDataOutputer outputer : outputerList)
		{
			outputer.pushData(dataType, value);
		}
		return 0;
	}

	@Override
	public int pushData(int dataType, List<String> valueList)
	{
		for(IDataOutputer outputer : outputerList)
		{
			outputer.pushData(dataType, valueList);
		}
		return 0;
	}

	@Override
	public void clear()
	{
		for(IDataOutputer outputer : outputerList)
		{
			outputer.clear();
		}		
	}
   
	
	
	
}
