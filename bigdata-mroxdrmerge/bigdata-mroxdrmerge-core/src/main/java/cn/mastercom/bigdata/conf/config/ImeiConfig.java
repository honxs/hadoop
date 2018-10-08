package cn.mastercom.bigdata.conf.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class ImeiConfig
{
	private static ImeiConfig instance;
	private HashMap<Integer, Integer> ImeiCapbilityMap;

	public static ImeiConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new ImeiConfig();
		}
		return instance;
	}

	private ImeiConfig()
	{
		ImeiCapbilityMap = new HashMap<Integer, Integer>();
	}

	public boolean loadImeiCapbility(Configuration conf)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getImeiConfigPath();
		try {
			FileReader.readFile(conf, filePath, new LineHandler() {
				
				@Override
				public void handle(String line) {
					if (line.trim().length() == 0)
					{
						return;
					}
					try
					{
						String[] val = line.split("\t");
						if (val.length == 2)
						{
							ImeiCapbilityMap.put(Integer.parseInt(val[0]), Integer.parseInt(val[1]));
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error,"imeiconfig  error1", "imeiconfig  error : " +
								line, e);
						return;
					}
					
				}
			});
		} catch (Exception e1) {
			
			LOGHelper.GetLogger().writeLog(LogType.error,"imeiconfig  error2", "imeiconfig  error ", e1);
			return false;
		}
//		try
//		{
//			BufferedReader reader = null;
//			try
//			{
//				if (!filePath.contains(":"))
//				{
//					HDFSOper hdfsOper = new HDFSOper(conf);
//					if (!hdfsOper.checkFileExist(filePath))
//					{
//						LOGHelper.GetLogger().writeLog(LogType.error, "imeiconfig  is not exists: " + filePath);
//						return false;
//					}
//					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
//				}
//				else if (new File(filePath).exists())
//				{
//					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
//				}
//				else
//				{
//					return false;
//				}
//				String strData = "";
//				while ((strData = reader.readLine()) != null)
//				{
//					if (strData.trim().length() == 0)
//					{
//						continue;
//					}
//					try
//					{
//						String[] val = strData.split("\t");
//						if (val.length == 2)
//						{
//							ImeiCapbilityMap.put(Integer.parseInt(val[0]), Integer.parseInt(val[1]));
//						}
//					}
//					catch (Exception e)
//					{
//						LOGHelper.GetLogger().writeLog(LogType.error, "imeiconfig  error : " + strData, e);
//						return false;
//					}
//				}
//			}
//			finally
//			{
//				if (reader != null)
//				{
//					reader.close();
//				}
//			}
//		}
//		catch (Exception e)
//		{
//			LOGHelper.GetLogger().writeLog(LogType.error, "imeiconfig  error ", e);
//			return false;
//		}
		return true;
	}

	public int getValue(int imei)
	{
		if (ImeiCapbilityMap.get(imei) == null)
		{
			return 0;
		}
		else
		{
			return ImeiCapbilityMap.get(imei);
		}
	}

	public int getValue(DT_Sample_4G sample)
	{
		int val = 0;
		if (ImeiCapbilityMap.get(sample.imeiTac) != null)
		{
			val = ImeiCapbilityMap.get(sample.imeiTac);
		}
		if (sample.dx_freq[0].LteNcEarfcn > 0 && sample.lt_freq[0].LteNcEarfcn > 0)
		{
			return val | 3;
		}
		else if (sample.lt_freq[0].LteNcEarfcn > 0)
		{
			return val | 2;
		}
		else if (sample.dx_freq[0].LteNcEarfcn > 0)
		{
			return val | 1;
		}
		else
		{
			return val;
		}
	}

	public int getValue(SIGNAL_MR_All mrall)
	{
		int val = 0;
		if (ImeiCapbilityMap.get(mrall.tsc.imeiTac) != null)
		{
			val = ImeiCapbilityMap.get(mrall.tsc.imeiTac);
		}
		if (mrall.dx_freq[0].LteNcEarfcn > 0 && mrall.lt_freq[0].LteNcEarfcn > 0)
		{
			return val | 3;
		}
		else if (mrall.lt_freq[0].LteNcEarfcn > 0)
		{
			return val | 2;
		}
		else if (mrall.dx_freq[0].LteNcEarfcn > 0)
		{
			return val | 1;
		}
		else
		{
			return val;
		}
	}
}
