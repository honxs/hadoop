package cn.mastercom.bigdata.stat.eciFilter;

import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.stat.tableinfo.enums.SingleProgEnums;
import cn.mastercom.bigdata.util.ResultOutputer;

public class EciFilterDeal
{
	public static final int DATATYPE_MME = 0;
	public static final int DATATYPE_HTTP = 1;
	public static final int DATATYPE_MRO = 2;
	public static final int DATATYPE_XDRLOCATION = 3;
	
	private ResultOutputer resultOutputer;

	public EciFilterDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
	}

	public void deal(EciFilterKey key, Iterable<Text> values)
	{
		try
		{
			for (Text temp : values)
			{
				if (key.getDataType() == DATATYPE_MME)
				{
					resultOutputer.pushData(SingleProgEnums.EciFilter_MME.getIndex(), temp.toString());
				}
				else if (key.getDataType() == DATATYPE_HTTP)
				{
					resultOutputer.pushData(SingleProgEnums.EciFilter_HTTP.getIndex(), temp.toString());
				}
				else if (key.getDataType() == DATATYPE_MRO)
				{
					resultOutputer.pushData(SingleProgEnums.EciFilter_MRO.getIndex(), temp.toString());
				}
				else
				{
					resultOutputer.pushData(SingleProgEnums.EciFilter_XDRLOCATION.getIndex(), temp.toString());
				}
				}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}
}
