package cn.mastercom.bigdata.conf.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class FilterCellConfig
{
	private static FilterCellConfig instance;

	public static FilterCellConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new FilterCellConfig();
		}
		return instance;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (!FilterCellConfig.GetInstance().loadFilterCell(conf, "180115"))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "filterCell init error 请检查！");
			throw (new IOException("filterCell init error 请检查！"));
		}
		System.out.println(FilterCellConfig.GetInstance().getLteCell(12345));
		System.out.println(FilterCellConfig.GetInstance().getLteCell(1234));
	}

	private FilterCellConfig()
	{
		lteCellInfoMap = new HashMap<Long, Integer>();
		leaderLteCellMap = new HashMap<Long, Integer>();
	}

	////////////////////////////////////////////////// tdlte
	private HashMap<Long, Integer> lteCellInfoMap;
	private static HashMap<Long, Integer> leaderLteCellMap;

	public String errLog = "";

	public HashMap<Long, Integer> getLteCellInfoList()
	{
		return lteCellInfoMap;
	}
	
/*	public boolean loadFilterCell(Configuration conf)
	{
		String filePath = "";
		filePath = MainModel.GetInstance().getAppConfig().getLeaderCellConfigPath();
		try
		{
			BufferedReader reader = null;
			try
			{
				if (!filePath.contains(":"))
				{
					HDFSOper hdfsOper = new HDFSOper(conf);
					if (!hdfsOper.checkFileExist(filePath))
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "config is not exists: " + filePath);
						System.out.println("path not exist :" + filePath);
						return false;
					}
					System.out.println("path exist :" + filePath);

					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				}
				else
				{
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
				}
				String strData;
				long eci;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.trim().length() == 0)
					{
						continue;
					}
					String[] value = strData.split("\t",-1);
					try
					{
						if (value.length > 1)
						{
							int enbid = Integer.parseInt(value[0]);
							int ci = Integer.parseInt(value[1]);
							eci = enbid * 256L + ci;
						}
						else
						{
							eci = Long.parseLong(strData);
						}
						if (leaderLteCellMap.containsKey(eci))
						{
							continue;
						}
						leaderLteCellMap.put(eci, 1);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error : " + strData, e);
						errLog = "loadFilterCell error : " + e.getMessage() + ":" + strData;
						e.printStackTrace();
						return false;
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error ", e);
				errLog = "loadFilterCell error : " + e.getMessage();
				e.printStackTrace();
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
			LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error ", e);
			errLog = "loadFilterCell error : " + e.getMessage();
			e.printStackTrace();
			return false;
		}
		System.out.println("eciFilterCell.size=" + lteCellInfoMap.size());

		return true;
	}
*/    
	public boolean loadSpecifiedCell(Configuration conf, String filePath)
	{
		try {
			FileReader.readFile(conf, filePath, new LineHandler() {
				long eci;
				@Override
				public void handle(String line) {
					if (line.trim().length() == 0)
					{
						return;
					}
					String[] value = line.split("\t",-1);
					try
					{
						if (value.length > 1)
						{
							int enbid = Integer.parseInt(value[0]);
							int ci = Integer.parseInt(value[1]);
							eci = enbid * 256L + ci;
						}
						else
						{
							eci = Long.parseLong(line);
						}
						if (leaderLteCellMap.containsKey(eci))
						{
							return;
						}
						leaderLteCellMap.put(eci, 1);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error,"loadSpecifiedCell error1", "loadSpecifiedCell " +
								"error : " + line, e);
						errLog = "loadSpecifiedCell error : " + e.getMessage() + ":" + line;
						e.printStackTrace();
						return;
					}
				}
			});
		} catch (Exception e1) {
			LOGHelper.GetLogger().writeLog(LogType.error,"loadSpecifiedCell error2", "loadSpecifiedCell error ", e1);
			errLog = "loadSpecifiedCell error : " + e1.getMessage();
			e1.printStackTrace();
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
//						LOGHelper.GetLogger().writeLog(LogType.error, "config is not exists: " + filePath);
//						System.out.println("path not exist :" + filePath);
//						return false;
//					}
//					System.out.println("path exist :" + filePath);
//
//					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
//				}
//				else
//				{
//					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
//				}
//				String strData;
//				long eci;
//				while ((strData = reader.readLine()) != null)
//				{
//					if (strData.trim().length() == 0)
//					{
//						continue;
//					}
//					String[] value = strData.split("\t",-1);
//					try
//					{
//						if (value.length > 1)
//						{
//							int enbid = Integer.parseInt(value[0]);
//							int ci = Integer.parseInt(value[1]);
//							eci = enbid * 256L + ci;
//						}
//						else
//						{
//							eci = Long.parseLong(strData);
//						}
//						if (leaderLteCellMap.containsKey(eci))
//						{
//							continue;
//						}
//						leaderLteCellMap.put(eci, 1);
//					}
//					catch (Exception e)
//					{
//						LOGHelper.GetLogger().writeLog(LogType.error, "loadSpecifiedCell error : " + strData, e);
//						errLog = "loadSpecifiedCell error : " + e.getMessage() + ":" + strData;
//						e.printStackTrace();
//						return false;
//					}
//				}
//			}
//			catch (Exception e)
//			{
//				LOGHelper.GetLogger().writeLog(LogType.error, "loadSpecifiedCell error ", e);
//				errLog = "loadSpecifiedCell error : " + e.getMessage();
//				e.printStackTrace();
//				return false;
//			}
//			finally
//			{
//				if (reader != null)
//				{
//					reader.close();
//				}
//			}
//
//		}
//		catch (Exception e)
//		{
//			LOGHelper.GetLogger().writeLog(LogType.error, "loadSpecifiedCell error ", e);
//			errLog = "loadSpecifiedCell error : " + e.getMessage();
//			e.printStackTrace();
//			return false;
//		}
		System.out.println("SpecifiedCell.size=" + leaderLteCellMap.size());

		return true;
	}


	public boolean loadFilterCell(Configuration conf, String date)
	{
		String filePath = "";
		if (date.length() > 0)
		{
			String temp = MainModel.GetInstance().getAppConfig().getFilterCellConfigPath();
			filePath = temp + "/eciFileter_" + date.replace("01_", "") + ".txt";
		}
		else
		{
			filePath = MainModel.GetInstance().getAppConfig().getSceneCellConfigPath();
		}
		try {
			FileReader.readFile(conf, filePath, new LineHandler() {
				long eci;
				@Override
				public void handle(String line) {
					try
					{
						if (line.trim().length() == 0)
						{
							return;
						}
						String [] value = line.split("\t", -1);
					
						if (value.length > 1)
						{
							int enbid = Integer.parseInt(value[0]);
							int ci = Integer.parseInt(value[1]);
							eci = enbid * 256L + ci;
						}
						else
						{
							eci = Long.parseLong(line);
						}
						if (lteCellInfoMap.containsKey(eci))
						{
							return;
						}
						lteCellInfoMap.put(eci, 1);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error,"loadFilterCell error", "loadFilterCell error : " + line, e);
						errLog = "loadFilterCell error : " + e.getMessage() + ":" + line;
						e.printStackTrace();
						return;
					}
					
				}
			});
		} catch (Exception e1) {
			LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error : " + e1.getMessage());
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
//						LOGHelper.GetLogger().writeLog(LogType.error, "config is not exists: " + filePath);
//						System.out.println("path not exist :" + filePath);
//						return false;
//					}
//					System.out.println("path exist :" + filePath);
//
//					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
//				}
//				else
//				{
//					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
//				}
//				String strData;
//				long eci;
//				while ((strData = reader.readLine()) != null)
//				{
//					if (strData.trim().length() == 0)
//					{
//						continue;
//					}
//					String[] value = strData.split("\t",-1);
//					try
//					{
//						if (value.length > 1)
//						{
//							int enbid = Integer.parseInt(value[0]);
//							int ci = Integer.parseInt(value[1]);
//							eci = enbid * 256L + ci;
//						}
//						else
//						{
//							eci = Long.parseLong(strData);
//						}
//						if (lteCellInfoMap.containsKey(eci))
//						{
//							continue;
//						}
//						lteCellInfoMap.put(eci, 1);
//					}
//					catch (Exception e)
//					{
//						LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error : " + strData, e);
//						errLog = "loadFilterCell error : " + e.getMessage() + ":" + strData;
//						e.printStackTrace();
//						return false;
//					}
//				}
//			}
//			catch (Exception e)
//			{
//				LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error ", e);
//				errLog = "loadFilterCell error : " + e.getMessage();
//				e.printStackTrace();
//				return false;
//			}
//			finally
//			{
//				if (reader != null)
//				{
//					reader.close();
//				}
//			}
//
//		}
//		catch (Exception e)
//		{
//			LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error ", e);
//			errLog = "loadFilterCell error : " + e.getMessage();
//			e.printStackTrace();
//			return false;
//		}
//		System.out.println("eciFilterCell.size=" + lteCellInfoMap.size());

		return true;
	}

	public boolean loadSceneCell(Configuration conf)
	{
		return loadFilterCell(conf,"");
	}

	
	public boolean getLteCell(long eci)
	{
		if (lteCellInfoMap.size() == 0 || lteCellInfoMap.containsKey(eci))//计算指定小区，否则全计算
		{
			return true;
		}
		return false;
	}
	public boolean getLteCell(long eci, boolean filter)
	{
		if (filter && lteCellInfoMap.containsKey(eci)) //过滤掉指定的小区，否则不过滤，全计算
		{
			return true;
		}
		return false;
	}
	
	public boolean getLeaderLteCell(long eci)
	{
		if (leaderLteCellMap.size() == 0 || leaderLteCellMap.containsKey(eci))//计算指定小区，否则全计算
		{
			return true;
		}
		return false;
	}
}
