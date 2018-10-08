package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.io.IOException;

import cn.mastercom.bigdata.StructData.MroOrigDataMT;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class MapByMroFormat_MrAll implements org.apache.spark.api.java.function.Function2<MroOrigDataMT, MroOrigDataMT, MroOrigDataMT>
{
	private static final long serialVersionUID = 1L;
	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader;
	
	public MapByMroFormat_MrAll() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		dataAdapterReader = new DataAdapterReader(parseItem);
	}
	
	
	
	@Override
	public MroOrigDataMT call(MroOrigDataMT arg0, MroOrigDataMT arg1) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
