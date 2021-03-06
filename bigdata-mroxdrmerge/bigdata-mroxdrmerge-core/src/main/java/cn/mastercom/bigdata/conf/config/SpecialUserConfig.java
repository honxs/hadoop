package cn.mastercom.bigdata.conf.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.StringUtil;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;

public class SpecialUserConfig
{
	private static SpecialUserConfig instance;
	private HashMap<Long, Integer> SpecialuserMap;

	private SpecialUserConfig()
	{
		SpecialuserMap = new HashMap<Long, Integer>();
	}

	public static SpecialUserConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new SpecialUserConfig();
		}
		return instance;
	}

	/**
	 * 
	 * @param imsi
	 * @param EncryptUser
	 *            是否加密
	 * @return
	 */
	public boolean ifSpeciUser(Long imsi, boolean EncryptUser)
	{
		long key = 0L;
		key = imsi;
		if (SpecialuserMap.get(key) == null)
		{
			return false;
		}
		return true;
	}

	public static void main(String args[])
	{
		SpecialUserConfig.GetInstance().loadSpecialuser(null, false);
		for (Long imsi : SpecialUserConfig.GetInstance().SpecialuserMap.keySet())
		{
			System.out.println(imsi);
		}
		System.out.println(SpecialUserConfig.GetInstance().ifSpeciUser(12345678L, false));
	}

	/**
	 * 
	 * @param conf
	 * @param EncryptUser
	 *            是否加密
	 * @return
	 */
	public boolean loadSpecialuser(Configuration conf, boolean EncryptUser)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getSpecialUserPath();
		
		try {
			FileReader.readFile(conf, filePath, new LineHandler() {
				
				@Override
				public void handle(String line) {
					if (line.trim().length() == 0)
					{
						return;
					}
					Long imsi = Long.parseLong(line.trim());
					SpecialuserMap.put(imsi, 1);
				}
			});
			return true;
		} catch (Exception e1) {
			LOGHelper.GetLogger().writeLog(LogType.error,"specialUser  error1", "specialUser  error ", e1);
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
//						LOGHelper.GetLogger().writeLog(LogType.error, "specialUserPath  is not exists: " + filePath);
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
//					Long imsi = Long.parseLong(strData.trim());
//					SpecialuserMap.put(imsi, 1);
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
//			LOGHelper.GetLogger().writeLog(LogType.error, "specialUser  error ", e);
//			return false;
//		}
		

	}

}
