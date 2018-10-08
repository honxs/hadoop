package cn.mastercom.bigdata.conf.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class HomeBroadbandConfig
{
	private static HomeBroadbandConfig instance;

	public static HomeBroadbandConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new HomeBroadbandConfig();
		}
		return instance;
	}

	private HomeBroadbandConfig()
	{
		homeBroadbandItemMap = new HashMap<String, List<HomeBroadbandItem>>();
	}

	public static void main(String[] args)
	{
		HomeBroadbandConfig.GetInstance().loadHomeBroadband(null, "E:\\mt_wlyh\\test\\homeBroad\\homeBroad.txt");
		for (String key : HomeBroadbandConfig.GetInstance().getHomeBroadbandItemMap().keySet())
		{
			System.out.println(key);
			for (HomeBroadbandItem item : HomeBroadbandConfig.GetInstance().getHomeBroadbandItemMap().get(key))
			{
				System.out.println("---" + item.toString());
			}
			System.out.println();
		}
	}

	public Map<String, List<HomeBroadbandItem>> homeBroadbandItemMap;
	public String errLog = "";

	public Map<String, List<HomeBroadbandItem>> getHomeBroadbandItemMap()
	{
		return homeBroadbandItemMap;
	}

	public boolean loadHomeBroadband(Configuration conf)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getHomeBroadbandConfigPath();
		return loadHomeBroadband(conf, filePath);
	}

	// "/mt_wlyh/Data/config/homebroadband.txt";
	public boolean loadHomeBroadband(Configuration conf, String filePath)
	{
		try
		{
			BufferedReader reader = null;
			homeBroadbandItemMap = new HashMap<String, List<HomeBroadbandItem>>();
			try
			{
				if (!filePath.contains(":"))
				{
					HDFSOper hdfsOper = new HDFSOper(conf);
					if (!hdfsOper.checkFileExist(filePath))
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "config is not exists: " + filePath);
						return false;
					}

					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				}
				else
				{
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
				}
				String strData;
				String[] values;
				String msisdn = "";
				List<HomeBroadbandItem> homeBroadbandList = null;
				while ((strData = reader.readLine()) != null)
				{
					try
					{
						values = strData.split(",|\t|&\\|", -1);
						if (values.length < 6)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "HomeBroadband config error: " + strData);
							continue;
						}
						HomeBroadbandItem item = HomeBroadbandItem.fillData(values);

						//过滤家宽定位精度
						if (item.reliability < 7)
						{
							continue;
						}
						
						//过滤家宽定位级别
						if (item.level < 9)
						{
							continue;
						}
						
						// 格式化手机号，取后11位
						msisdn = item.msisdn.replace(" ", "");
						if (msisdn.length() < 11)
						{
							continue;
						}
						if (msisdn.startsWith("86") || msisdn.startsWith("+86"))
						{
							msisdn = msisdn.substring(msisdn.length() - 11, msisdn.length());
						}
						
						homeBroadbandList = homeBroadbandItemMap.get(msisdn);
						if (homeBroadbandList == null)
						{
							homeBroadbandList = new ArrayList<HomeBroadbandItem>();
							homeBroadbandItemMap.put(msisdn, homeBroadbandList);
						}
						homeBroadbandList.add(item);
					}
					catch (Exception e)
					{
						e.printStackTrace();
						LOGHelper.GetLogger().writeLog(LogType.error,"loadHomeBroadband error1", "loadHomeBroadband " +
								"error : " + strData, e);
						errLog = "loadHomeBroadband error : " + e.getMessage() + ":" + strData;
						return false;
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(LogType.error,"loadHomeBroadband error2", "loadHomeBroadband error ", e);
				errLog = "loadHomeBroadband error : " + e.getMessage();
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			LOGHelper.GetLogger().writeLog(LogType.error,"loadHomeBroadband error3", "loadHomeBroadband error ", e);
			errLog = "loadHomeBroadband error : " + e.getMessage();
			return false;
		}

		return true;
	}
}
