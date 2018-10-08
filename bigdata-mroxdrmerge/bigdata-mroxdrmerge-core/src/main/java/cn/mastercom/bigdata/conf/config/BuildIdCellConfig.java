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

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class BuildIdCellConfig
{
	private static BuildIdCellConfig instance;

	public static BuildIdCellConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new BuildIdCellConfig();
		}
		return instance;
	}

	public static void main(String[] args) throws Exception
	{
		BuildIdCellConfig.GetInstance().loadBuildIdCell(null, "D:\\test\\config\\buildidcell.txt");
		System.out.println(BuildIdCellConfig.GetInstance().buildIdCellInfoMap.size());
	}

	private BuildIdCellConfig()
	{
		buildIdCellInfoMap = new HashMap<Integer, List<Long>>();
	}

	public Map<Integer, List<Long>> buildIdCellInfoMap;

	public String errLog = "";

	public Map<Integer, List<Long>> getBuildIdCellInfoMap()
	{
		return buildIdCellInfoMap;
	}

	public boolean loadBuildIdCell(Configuration conf)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getBuildIdCellConfigPath();
		return loadBuildIdCell(conf, filePath);
	}

	// "/mt_wlyh/Data/config/buildidcell.txt";
	public boolean loadBuildIdCell(Configuration conf, String filePath)
	{
		try
		{
			BufferedReader reader = null;
			buildIdCellInfoMap = new HashMap<Integer, List<Long>>();
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
				int buildId = 0;
				List<Long> eciList = null;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.trim().length() == 0)
					{
						continue;
					}
					try
					{
						values = strData.split("\t", -1);// |\t
						if (values.length < 2)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "buildidcell config error: " + strData);
							continue;
						}
						BuildIdCellInfo item = BuildIdCellInfo.FillData(values);
						buildId = item.buildid;
						eciList = buildIdCellInfoMap.get(buildId);
						if(eciList == null)
						{
							eciList = new ArrayList<Long>();
							buildIdCellInfoMap.put(buildId, eciList);
						}
						eciList.add(item.eci);
					}
					catch (Exception e)
					{
						e.printStackTrace();
						LOGHelper.GetLogger().writeLog(LogType.error,"loadBuildIdCell error1", "loadBuildIdCell error" +
								" : " + strData, e);
						errLog = "loadBuildIdCell error : " + e.getMessage() + ":" + strData;
						return false;
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(LogType.error,"loadBuildIdCell error2", "loadBuildIdCell error ", e);
				errLog = "loadBuildIdCell error : " + e.getMessage();
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
			LOGHelper.GetLogger().writeLog(LogType.error,"loadBuildIdCell error3", "loadBuildIdCell error ", e);
			errLog = "loadBuildIdCell error : " + e.getMessage();
			return false;
		}

		return true;
	}
}
