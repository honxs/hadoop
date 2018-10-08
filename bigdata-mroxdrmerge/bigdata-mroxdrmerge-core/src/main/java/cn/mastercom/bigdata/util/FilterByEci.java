package cn.mastercom.bigdata.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.conf.config.FilterCellConfig;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;

public class FilterByEci
{
	public static void readEciList(Configuration conf, String date) throws IOException
	{
		if (!FilterCellConfig.GetInstance().loadFilterCell(conf, date))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "filtercell init error 请检查！" + FilterCellConfig.GetInstance().errLog);
		}
	}

	public static boolean ifMap(long eci)
	{
		return FilterCellConfig.GetInstance().getLteCell(eci);
	}
}
