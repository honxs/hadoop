package cn.mastercom.bigdata.stat.freqloc;

import java.util.ArrayList;

import cn.mastercom.bigdata.stat.tableinfo.enums.SingleProgEnums;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

import org.apache.hadoop.io.Text;

public class FreqLocDeal
{
	private ResultOutputer resultOutputer;

	public FreqLocDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
	}

	public void deal(FreqLocKey key, Iterable<Text> values)
	{
		ArrayList<FreqCellStruct> list = new ArrayList<>();
		list = FreqLocCluster.cluster(values);
		for (FreqCellStruct freqcellStruct : list)
		{
			for (FreqCellItem temp : freqcellStruct.map.values())
			{
				try
				{
					temp.longitude = freqcellStruct.aveLongitude;
					temp.latitude = freqcellStruct.aveLatitude;
					resultOutputer.pushData(SingleProgEnums.FreqLoc.getIndex(), key.getCityId() + "\t" + key.getFreq() + "\t" + temp.toLine());
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"freqLoc err", "freqLoc err ", e);
				}
			}
		}
	}
}
